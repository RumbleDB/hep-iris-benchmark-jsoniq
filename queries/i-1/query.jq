import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := parquet-file($dataPath).MET_sumet

return hep:histogram($filtered, 0, 2000, 100)
