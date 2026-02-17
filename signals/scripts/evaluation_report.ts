import { mkdir, readFile, writeFile } from "fs/promises"
import path from "path"
import {
  buildEvaluationReport,
  normalizeEvaluationRows,
  parseEvaluationReportArgs,
  renderEvaluationReportMarkdown,
} from "../src/ops/evaluation_report"

async function run() {
  const args = parseEvaluationReportArgs(process.argv)

  const inputPath = path.resolve(process.cwd(), args.inputPath)
  const outDir = path.resolve(process.cwd(), args.outDir)

  const raw = await readFile(inputPath, "utf8")
  const parsed = JSON.parse(raw) as unknown
  const rows = normalizeEvaluationRows(parsed)

  const report = buildEvaluationReport(rows, {
    minTrainSize: args.minTrainSize,
    testSize: args.testSize,
    stepSize: args.stepSize,
    expandingWindow: args.expandingWindow,
    maxSlices: args.maxSlices,
    familywiseAlpha: args.familywiseAlpha,
    minimumRecommendedSampleSize: args.minimumRecommendedSampleSize,
    minResolvedObservations: args.minResolvedObservations,
    maxUnresolvedRatePct: args.maxUnresolvedRatePct,
    minSliceCount: args.minSliceCount,
  })

  const markdown = renderEvaluationReportMarkdown(report)

  await mkdir(outDir, { recursive: true })
  await writeFile(path.join(outDir, "report.json"), JSON.stringify(report, null, 2))
  await writeFile(path.join(outDir, "report.md"), markdown)

  console.log(`Evaluation report generated at ${outDir}`)
  console.log(`Rows parsed: ${rows.length}`)
  console.log(`Walk-forward slices: ${report.walk_forward.slice_count}`)
  console.log(`Hypotheses tested: ${report.multiple_testing.hypotheses.length}`)
  console.log(`Governance status: ${report.governance_status.status.toUpperCase()}`)

  if (report.governance_status.status === "fail") {
    for (const reason of report.governance_status.reasons) {
      console.error(`Governance fail: ${reason}`)
    }
    process.exitCode = 1
  }
}

run().catch((error) => {
  console.error(error)
  process.exit(1)
})
