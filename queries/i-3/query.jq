import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(15, 60, 100)

let $filtered := (
  for $i in parquet-file($dataPath)
  let $pointFiltered := (
    for $j in (1 to size($i.Jet_pt))
    where abs($i.Jet_eta[[$j]]) < 1
    return $i.Jet_pt[[$j]]
  )
  return $pointFiltered
)

return hep:buildHistogram($filtered, $histogram)
