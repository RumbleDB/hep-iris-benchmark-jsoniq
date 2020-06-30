import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(15, 40, 100)

let $filtered := (
  for $i in hep:RestructureDataParquet($dataPath)
  where $i.nJet > 2

  let $triplets := (
    for $j1 in $i.jets[]
    for $j2 in $i.jets[]
    for $j3 in $i.jets[]
    where $j1.idx < $j2.idx and $j2.idx < $j3.idx
    let $triJetMass := abs(172.5 - hep:TriJet($j1, $j2, $j3).mass)
    order by $triJetMass
    count $c
    where $c <= 1
    return ($j1.pt, $j2.pt, $j3.pt)
  )

  return $triplets
)

return hep:buildHistogram($filtered, $histogram)
