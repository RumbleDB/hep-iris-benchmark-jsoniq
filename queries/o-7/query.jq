import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in hep:RestructureDataParquet($dataPath)

  let $filteredJets := (
    for $jet in $event.jets[]
    where $jet.pt > 30

    let $leptons := hep:ConcatLeptons($event)
    where empty(
      for $lepton in $leptons
      where $lepton.pt > 10 and hep:DeltaR($jet, $lepton) < 40
      return {}
    )

    return $jet
  )

  where exists($filteredJets)
  return sum($filteredJets.pt)
)

return hep:histogram($filtered, 15, 200, 100)
