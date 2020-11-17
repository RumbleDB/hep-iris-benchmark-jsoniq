import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(15, 60, 100)

let $filtered := (
  for $event in parquet-file($dataPath)
  for $i in (1 to size($event.Jet_pt))
  where abs($event.Jet_eta[[$i]]) < 1
  return $event.Jet_pt[[$i]]
)

return hep:buildHistogram($filtered, $histogram)
