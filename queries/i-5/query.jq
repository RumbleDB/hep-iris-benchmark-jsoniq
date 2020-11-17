import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in parquet-file($dataPath)
  where $event.nMuon > 1
  where exists(
    for $i in (1 to (size($event.Muon_charge) - 1))
    for $j in (($i + 1) to size($event.Muon_charge))
    where $event.Muon_charge[[$i]] != $event.Muon_charge[[$j]]
    let $invariantMass := hep-i:computeInvariantMass($event, $i, $j)
    where 60 < $invariantMass and $invariantMass < 120
    return {}
  )
  return $event.MET_sumet
)

return hep:histogram($filtered, 0, 2000, 100)
