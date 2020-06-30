import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(0, 1, 100)

let $filtered := (
  for $i in hep:RestructureDataParquet($dataPath)
  where $i.nJet > 2

  let $maxBtag := (
    for $j1 in $i.jets[]
    for $j2 in $i.jets[]
    for $j3 in $i.jets[]
    where $j1.idx < $j2.idx and $j2.idx < $j3.idx
    let $triJetMass := abs(172.5 - hep:TriJet($j1, $j2, $j3).mass)
    order by $triJetMass
    count $c
    where $c <= 1
    return max(($j1.btag, $j2.btag, $j3.btag))
  )

  return $maxBtag
)

return hep:buildHistogram($filtered, $histogram)
