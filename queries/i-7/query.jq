import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in parquet-file($dataPath)
  let $filteredJets := (
    for $jetIdx in (1 to size($event.Jet_pt))
    where $event.Jet_pt[[$jetIdx]] > 30

    let $filteredElectrons := (
      for $electronIdx in (1 to size($event.Electron_pt))
      let $deltaR := hep-i:DeltaR(
        $event.Jet_phi[[$jetIdx]],
        $event.Electron_phi[[$electronIdx]],
        $event.Jet_eta[[$jetIdx]],
        $event.Electron_eta[[$electronIdx]])
      where $event.Electron_pt[[$electronIdx]] > 10 and $deltaR < 40
      return $electronIdx
    )
    where empty($filteredElectrons)

    let $filteredMuons := (
      for $muonIdx in (1 to size($event.Muon_pt))
      let $deltaR := hep-i:DeltaR(
        $event.Jet_phi[[$jetIdx]],
        $event.Muon_phi[[$muonIdx]],
        $event.Jet_eta[[$jetIdx]],
        $event.Muon_eta[[$muonIdx]])
      where $event.Muon_pt[[$muonIdx]] > 10 and $deltaR < 40
      return $muonIdx
    )
    where empty($filteredMuons)

    return $event.Jet_pt[[$jetIdx]]
  )
  where exists($filteredJets)

  return sum($filteredJets)
)

return hep:histogram($filtered, 15, 200, 100)
