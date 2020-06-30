import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(0, 2000, 100)

let $filtered := (
  for $i in parquet-file($dataPath)
  where $i.nMuon > 1
  let $pairs := (
    for $iIdx in (1 to (size($i.Muon_charge) - 1))
    return for $jIdx in (($iIdx + 1) to size($i.Muon_charge))
           where $i.Muon_charge[[$iIdx]] != $i.Muon_charge[[$jIdx]]
           return [$iIdx, $jIdx]
  )
  where exists($pairs)

  let $temp := (
    for $pair in $pairs
    let $invariantMass := hep-i:computeInvariantMass($i, $pair[[1]], $pair[[2]])
    where 60 < $invariantMass and $invariantMass < 120
    return $invariantMass
  )
  where exists($temp)
  return $i.MET_sumet
)

return hep:buildHistogram($filtered, $histogram)
