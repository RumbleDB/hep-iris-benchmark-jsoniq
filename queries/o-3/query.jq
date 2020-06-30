import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(15, 60, 100)

let $filtered := (
  for $i in hep:RestructureDataParquet($dataPath).jets[]
  where abs($i.eta) < 1
  return $i.pt
)

return hep:buildHistogram($filtered, $histogram)
