import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(0, 2000, 100)

let $filtered := parquet-file($dataPath).MET_sumet

return hep:buildHistogram($filtered, $histogram)
