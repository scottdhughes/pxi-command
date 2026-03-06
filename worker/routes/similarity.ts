import type { WorkerRouteContext } from '../types';

type SimilarityDeps = Record<string, any>;

export async function tryHandleSimilarityRoute(
  route: WorkerRouteContext,
  deps: SimilarityDeps,
): Promise<Response | null> {
  const { env, url, method, corsHeaders } = route;

  if (url.pathname === '/api/similar' && method === 'GET') {
    try {
      const latestPxi = await env.DB.prepare(
        'SELECT date, score, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 1'
      ).first<{ date: string; score: number; delta_7d: number | null; delta_30d: number | null }>();

      if (!latestPxi) {
        return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
      }

      const [indicators, categories] = await Promise.all([
        env.DB.prepare(`
          SELECT indicator_id, value FROM indicator_values
          WHERE date = ? ORDER BY indicator_id
        `).bind(latestPxi.date).all<{ indicator_id: string; value: number }>(),
        env.DB.prepare(`
          SELECT category, score FROM category_scores
          WHERE date = ? ORDER BY category
        `).bind(latestPxi.date).all<{ category: string; score: number }>(),
      ]);

      if (!indicators.results || indicators.results.length === 0) {
        return Response.json({ error: 'No indicators' }, { status: 404, headers: corsHeaders });
      }

      const embeddingText = deps.generateEmbeddingText({
        indicators: indicators.results,
        pxi: {
          score: latestPxi.score,
          delta_7d: latestPxi.delta_7d,
          delta_30d: latestPxi.delta_30d,
        },
        categories: categories.results || [],
      });

      let embedding;
      try {
        embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
          text: embeddingText,
        });
      } catch (aiError) {
        console.error('Workers AI embedding error:', aiError);
        return Response.json({
          error: 'AI embedding failed',
          details: aiError instanceof Error ? aiError.message : String(aiError),
        }, { status: 503, headers: corsHeaders });
      }

      const embeddingVector = embedding ? deps.getEmbeddingVector(embedding) : null;
      if (!embeddingVector) {
        return Response.json({ error: 'Empty embedding response' }, { status: 500, headers: corsHeaders });
      }

      let similar;
      try {
        similar = await env.VECTORIZE.query(embeddingVector, {
          topK: 50,
          returnMetadata: 'all',
        });
      } catch (vecError) {
        console.error('Vectorize query error:', vecError);
        return Response.json({
          error: 'Vectorize query failed',
          details: vecError instanceof Error ? vecError.message : String(vecError),
        }, { status: 503, headers: corsHeaders });
      }

      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - 30);
      const cutoffDateStr = cutoffDate.toISOString().split('T')[0];

      const filteredMatches = (similar.matches || [])
        .filter((match: any) => {
          const matchDate = match.metadata?.date as string;
          return matchDate && matchDate < cutoffDateStr;
        })
        .slice(0, 5);

      const similarDates = filteredMatches
        .filter((match: any) => match.metadata?.date)
        .map((match: any) => match.metadata.date as string);

      if (similarDates.length === 0) {
        return Response.json({
          current_date: latestPxi.date,
          cutoff_date: cutoffDateStr,
          similar_periods: [],
          total_matches: similar.matches?.length || 0,
          message: 'No historical periods found (excluding last 30 days). Need more historical data.',
        }, { headers: corsHeaders });
      }

      const historicalScores = await env.DB.prepare(`
        SELECT date, score, label, status
        FROM pxi_scores
        WHERE date IN (${similarDates.map(() => '?').join(',')})
      `).bind(...similarDates).all<{
        date: string;
        score: number;
        label: string;
        status: string;
      }>();

      const embeddingReturns = await env.DB.prepare(`
        SELECT date, forward_return_7d, forward_return_30d
        FROM market_embeddings
        WHERE date IN (${similarDates.map(() => '?').join(',')})
      `).bind(...similarDates).all<{ date: string; forward_return_7d: number | null; forward_return_30d: number | null }>();

      const embeddingReturnMap = new Map(
        (embeddingReturns.results || []).map((row) => [row.date, row]),
      );

      const spyPrices = await env.DB.prepare(`
        SELECT date, value FROM indicator_values
        WHERE indicator_id = 'spy_close'
        ORDER BY date ASC
      `).all<{ date: string; value: number }>();

      const spyMap = new Map<string, number>();
      for (const price of spyPrices.results || []) {
        spyMap.set(price.date, price.value);
      }

      const getSpyPrice = (dateStr: string, maxDaysForward = 5): number | null => {
        const date = new Date(dateStr);
        for (let offset = 0; offset <= maxDaysForward; offset += 1) {
          const checkDate = new Date(date);
          checkDate.setDate(checkDate.getDate() + offset);
          const checkStr = checkDate.toISOString().split('T')[0];
          const price = spyMap.get(checkStr);
          if (price !== undefined) {
            return price;
          }
        }
        return null;
      };

      const calculateForwardReturn = (startDate: string, horizonDays: number): number | null => {
        const start = getSpyPrice(startDate);
        if (start === null) return null;

        const target = new Date(startDate);
        target.setDate(target.getDate() + horizonDays);
        const targetStr = target.toISOString().split('T')[0];
        const end = getSpyPrice(targetStr);
        if (end === null) return null;

        return ((end - start) / start) * 100;
      };

      const accuracyScores = await env.DB.prepare(`
        SELECT period_date, accuracy_score, times_used FROM period_accuracy
        WHERE period_date IN (${similarDates.map(() => '?').join(',')})
      `).bind(...similarDates).all<{ period_date: string; accuracy_score: number; times_used: number }>();

      const accuracyMap = new Map(
        (accuracyScores.results || []).map((row) => [row.period_date, { score: row.accuracy_score, used: row.times_used }]),
      );

      const todayMs = Date.now();

      return Response.json({
        current_date: latestPxi.date,
        cutoff_date: cutoffDateStr,
        similar_periods: filteredMatches.map((match: any) => {
          const historical = historicalScores.results?.find((row) => row.date === match.metadata?.date);
          const matchDate = match.metadata?.date as string;
          const similarityWeight = match.score;
          const daysSince = matchDate ? (todayMs - new Date(matchDate).getTime()) / (1000 * 60 * 60 * 24) : 0;
          const recencyWeight = Math.exp(-daysSince / 365);
          const periodAccuracy = matchDate ? accuracyMap.get(matchDate) : null;
          const accuracyWeight = periodAccuracy && periodAccuracy.used >= 2 ? periodAccuracy.score : 0.5;
          const combinedWeight = similarityWeight * (0.4 + 0.3 * recencyWeight + 0.3 * accuracyWeight);
          const embeddingReturn = matchDate ? embeddingReturnMap.get(matchDate) : null;
          const computed7d = matchDate ? calculateForwardReturn(matchDate, 7) : null;
          const computed30d = matchDate ? calculateForwardReturn(matchDate, 30) : null;

          return {
            date: matchDate,
            similarity: match.score,
            weights: {
              combined: combinedWeight,
              similarity: similarityWeight,
              recency: recencyWeight,
              accuracy: accuracyWeight,
              accuracy_sample: periodAccuracy?.used || 0,
            },
            pxi: historical ? {
              date: historical.date,
              score: historical.score,
              label: historical.label,
              status: historical.status,
            } : null,
            forward_returns: historical ? {
              d7: computed7d ?? embeddingReturn?.forward_return_7d ?? null,
              d30: computed30d ?? embeddingReturn?.forward_return_30d ?? null,
            } : null,
          };
        }),
      }, { headers: corsHeaders });
    } catch (err) {
      console.error('Similar endpoint error:', err);
      return Response.json({
        error: 'Similar search failed',
        details: err instanceof Error ? err.message : String(err),
      }, { status: 500, headers: corsHeaders });
    }
  }

  if (url.pathname === '/api/embed' && method === 'POST') {
    const dates = await env.DB.prepare(
      'SELECT DISTINCT date FROM indicator_values ORDER BY date'
    ).all<{ date: string }>();

    let embedded = 0;
    const batchSize = 10;

    for (let index = 0; index < (dates.results?.length || 0); index += batchSize) {
      const batch = dates.results!.slice(index, index + batchSize);
      for (const { date } of batch) {
        const indicators = await env.DB.prepare(`
          SELECT indicator_id, value FROM indicator_values
          WHERE date = ? ORDER BY indicator_id
        `).bind(date).all<{ indicator_id: string; value: number }>();

        if (!indicators.results || indicators.results.length < 10) continue;

        const indicatorText = indicators.results
          .map((indicator) => `${indicator.indicator_id}: ${indicator.value.toFixed(2)}`)
          .join(', ');

        const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
          text: indicatorText,
        });
        const embeddingVector = deps.getEmbeddingVector(embedding);

        await env.VECTORIZE.upsert([{
          id: date,
          values: embeddingVector,
          metadata: { date },
        }]);

        embedded += 1;
      }
    }

    return Response.json({
      success: true,
      embedded_dates: embedded,
    }, { headers: corsHeaders });
  }

  return null;
}
