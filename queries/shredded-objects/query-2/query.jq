import module namespace hep = "../../common/hep.jq";
declare variable $input-path as anyURI external := anyURI("../../../data/Run2012B_SingleMu.root");

let $filtered := parquet-file($input-path).Jet_pt[]

return hep:histogram($filtered, 15, 60, 100)
