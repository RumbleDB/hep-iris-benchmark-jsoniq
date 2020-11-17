import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in hep:RestructureDataParquet($dataPath)
  where $event.nJet > 2
  return (
    for $jet1 in $event.jets[]
    for $jet2 in $event.jets[]
    for $jet3 in $event.jets[]
    where $jet1.idx < $jet2.idx and $jet2.idx < $jet3.idx
    order by abs(172.5 - hep:TriJet($jet1, $jet2, $jet3).mass) ascending
    return [$jet1.pt, $jet2.pt, $jet3.pt]
  )[1][]
)

return hep:histogram($filtered, 15, 40, 100)
