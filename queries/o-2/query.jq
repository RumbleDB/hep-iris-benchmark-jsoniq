import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := parquet-file($dataPath).Jet_pt[]

return hep:histogram($filtered, 15, 60, 100)
