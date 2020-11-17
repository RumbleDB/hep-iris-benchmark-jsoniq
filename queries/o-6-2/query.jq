import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in hep:RestructureDataParquet($dataPath)
  where $event.nJet > 2

  let $maxBtag := (
    for $jet1 in $event.jets[]
    for $jet2 in $event.jets[]
    for $jet3 in $event.jets[]
    where $jet1.idx < $jet2.idx and $jet2.idx < $jet3.idx
    let $triJetMass := abs(172.5 - hep:TriJet($jet1, $jet2, $jet3).mass)
    order by $triJetMass
    count $c
    where $c <= 1
    return max(($jet1.btag, $jet2.btag, $jet3.btag))
  )

  return $maxBtag
)

return hep:histogram($filtered, 0, 1, 100)
