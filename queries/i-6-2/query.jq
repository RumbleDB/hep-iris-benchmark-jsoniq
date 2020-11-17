import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in parquet-file($dataPath)
  where $event.nJet > 2

  let $min-triplet-idxs := (
    for $i in (1 to (size($event.Jet_pt) - 2))
    for $j in (($i + 1) to (size($event.Jet_pt) - 1))
    for $k in (($j + 1) to size($event.Jet_pt))
    let $particleOne := hep-i:MakeJetParticle($event, $i)
    let $particleTwo := hep-i:MakeJetParticle($event, $j)
    let $particleThree := hep-i:MakeJetParticle($event, $k)
    let $triJet := hep:TriJet($particleOne, $particleTwo, $particleThree)
    order by abs(172.5 - $triJet.mass) ascending
    return [$i, $j, $k]
  )[1]

  let $btags := (
    for $i in $min-triplet-idxs[]
    return $event.Jet_btag[[$i]]
  )

  return max($btags)
)

return hep:histogram($filtered, 0, 1, 100)
