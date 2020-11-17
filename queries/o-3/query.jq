import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $jet in hep:RestructureDataParquet($dataPath).jets[]
  where abs($jet.eta) < 1
  return $jet.pt
)

return hep:histogram($filtered, 15, 60, 100)
