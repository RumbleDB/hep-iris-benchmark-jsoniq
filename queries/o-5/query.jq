import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(0, 2000, 100)

let $filtered := (
  for $i in hep:RestructureDataParquet($dataPath)
  where $i.nMuon > 1

  let $masses := (
    for $m1 in $i.muons[]
    for $m2 in $i.muons[]
    where $m1.idx < $m2.idx
    where $m1.charge != $m2.charge
    let $invariantMass := hep:computeInvariantMass($m1, $m2)
    where 60 < $invariantMass and $invariantMass < 120
    return $invariantMass
  )
  where exists($masses)

  return $i.MET_sumet
)

return hep:buildHistogram($filtered, $histogram)
