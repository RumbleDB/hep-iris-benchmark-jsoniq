import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(0, 2000, 100)

let $filtered := (
  for $event in parquet-file($dataPath)
  where $event.nMuon > 1
  let $pairs := (
    for $i in (1 to (size($event.Muon_charge) - 1))
    return for $j in (($i + 1) to size($event.Muon_charge))
           where $event.Muon_charge[[$i]] != $event.Muon_charge[[$j]]
           return [$i, $j]
  )
  where exists($pairs)

  let $temp := (
    for $pair in $pairs
    let $invariantMass := hep-i:computeInvariantMass($event, $pair[[1]], $pair[[2]])
    where 60 < $invariantMass and $invariantMass < 120
    return $invariantMass
  )
  where exists($temp)
  return $event.MET_sumet
)

return hep:buildHistogram($filtered, $histogram)
