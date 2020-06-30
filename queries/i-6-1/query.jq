import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(15, 40, 100)

let $filtered := (
  for $i in parquet-file($dataPath)
  where $i.nJet > 2
  let $triplets := (
    for $iIdx in (1 to (size($i.Jet_pt) - 2))
    return
      for $jIdx in (($iIdx + 1) to (size($i.Jet_pt) - 1))
      return
        for $kIdx in (($jIdx + 1) to size($i.Jet_pt))
        let $particleOne := hep-i:MakeJetParticle($i, $iIdx)
        let $particleTwo := hep-i:MakeJetParticle($i, $jIdx)
        let $particleThree := hep-i:MakeJetParticle($i, $kIdx)
        let $triJet := hep:TriJet($particleOne, $particleTwo, $particleThree)
        return {"idx": [$iIdx, $jIdx, $kIdx], "mass": abs(172.5 - $triJet.mass)}
  )

  let $minMass := min($triplets.mass)

  let $minTriplet := (
    for $j in $triplets
    where $j.mass = $minMass
    return $j
  )

  let $pT := (
    for $j in $minTriplet.idx[]
    return $i.Jet_pt[[$j]]
  )

  return $pT
)

return hep:buildHistogram($filtered, $histogram)
