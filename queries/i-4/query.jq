import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(0, 2000, 100)

let $filtered := (
  for $event in parquet-file($dataPath)
  let $subCount := count($event.Jet_pt[][$$ > 40])
  where $subCount > 1
  return $event.MET_sumet
)

return hep:buildHistogram($filtered, $histogram)
