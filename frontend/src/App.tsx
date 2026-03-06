import './App.css'

import { AlertHistoryPanel } from './components/AlertHistoryPanel'
import { OnboardingModal } from './components/home/OnboardingModal'
import { usePxiAppShell } from './hooks/usePxiAppShell'
import { HomePage } from './pages/HomePage'
import { BriefPage } from './pages/BriefPage'
import { InboxPage } from './pages/InboxPage'
import { OpportunitiesPage } from './pages/OpportunitiesPage'
import { SpecPage } from './pages/SpecPage'

function App() {
  const {
    route,
    data,
    prediction,
    signal,
    ensemble,
    mlAccuracy,
    historyData,
    historyRange,
    setHistoryRange,
    showOnboarding,
    setShowOnboarding,
    alertsData,
    selectedCategory,
    setSelectedCategory,
    signalsData,
    similarData,
    backtestData,
    planData,
    briefData,
    briefDecisionImpact,
    opportunitiesData,
    opportunitiesDecisionImpact,
    opsDecisionImpact,
    opportunityDiagnostics,
    edgeDiagnostics,
    opportunityHorizon,
    setOpportunityHorizon,
    alertsFeed,
    showSubscribeModal,
    setShowSubscribeModal,
    subscriptionNotice,
    setSubscriptionNotice,
    loading,
    error,
    menuOpen,
    setMenuOpen,
    menuRef,
    navigateTo,
    handleOpportunityCtaIntent,
  } = usePxiAppShell()

  if (route === '/spec') {
    return <SpecPage onClose={() => navigateTo('/')} inPage />
  }

  if (route === '/alerts') {
    return alertsData ? (
      <AlertHistoryPanel
        alerts={alertsData.alerts}
        accuracy={alertsData.accuracy}
        inPage
        onClose={() => navigateTo('/')}
      />
    ) : (
      <div className="min-h-screen bg-black text-[#949ba5] flex flex-col items-center justify-center px-4">
        <p className="text-sm uppercase tracking-widest">No alert history available yet.</p>
        <button
          onClick={() => navigateTo('/')}
          className="mt-4 text-[10px] uppercase tracking-[0.25em] border border-[#26272b] px-4 py-2 rounded"
        >
          Return Home
        </button>
      </div>
    )
  }

  if (route === '/guide') {
    return <OnboardingModal onClose={() => navigateTo('/')} inPage exampleScore={data?.score} />
  }

  if (route === '/brief') {
    return <BriefPage brief={briefData} decisionImpact={briefDecisionImpact} onBack={() => navigateTo('/')} />
  }

  if (route === '/opportunities') {
    return (
      <OpportunitiesPage
        data={opportunitiesData}
        decisionImpact={opportunitiesDecisionImpact}
        opsDecisionImpact={opsDecisionImpact}
        diagnostics={opportunityDiagnostics}
        edgeDiagnostics={edgeDiagnostics}
        horizon={opportunityHorizon}
        onHorizonChange={setOpportunityHorizon}
        onLogActionIntent={handleOpportunityCtaIntent}
        onBack={() => navigateTo('/')}
      />
    )
  }

  if (route === '/inbox') {
    return (
      <InboxPage
        alerts={alertsFeed?.alerts || []}
        onBack={() => navigateTo('/')}
        onOpenSubscribe={() => setShowSubscribeModal(true)}
        notice={subscriptionNotice}
      />
    )
  }

  if (loading) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-[#949ba5] text-sm tracking-widest uppercase animate-pulse">
          loading
        </div>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-[#949ba5] text-sm">
          {error || 'No data available'}
        </div>
      </div>
    )
  }

  return (
    <HomePage
      alertsData={alertsData}
      alertsFeed={alertsFeed}
      backtestData={backtestData}
      briefData={briefData}
      data={data}
      ensemble={ensemble}
      historyData={historyData}
      historyRange={historyRange}
      menuOpen={menuOpen}
      menuRef={menuRef}
      mlAccuracy={mlAccuracy}
      navigateTo={navigateTo}
      opportunitiesData={opportunitiesData}
      planData={planData}
      prediction={prediction}
      selectedCategory={selectedCategory}
      setHistoryRange={setHistoryRange}
      setMenuOpen={setMenuOpen}
      setSelectedCategory={setSelectedCategory}
      setShowOnboarding={setShowOnboarding}
      setShowSubscribeModal={setShowSubscribeModal}
      setSubscriptionNotice={setSubscriptionNotice}
      showOnboarding={showOnboarding}
      showSubscribeModal={showSubscribeModal}
      signal={signal}
      signalsData={signalsData}
      similarData={similarData}
      subscriptionNotice={subscriptionNotice}
    />
  )
}

export default App
