import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

declare function ConcatLeptons($event) {
  let $muons := (
    for $i in $event.muons[]
    return {| $i, {"type": "m"}  |}
  )  

  let $electrons := (
    for $i in $event.electrons[]
    return {| $i, {"type": "e"}  |}
  )  

  return ($muons, $electrons)
};

let $histogram := hep:histogramConsts(15, 60, 100)

let $filtered := (
  for $i in hep:RestructureDataParquet($dataPath)
  let $leptonSize := integer($i.nMuon + $i.nElectron)
  where $leptonSize > 2 

  let $leptons := ConcatLeptons($i)
  let $bestPair := (
    for $l1Idx in (1 to ($leptonSize - 1)) 
      for $l2Idx in (($l1Idx + 1) to $leptonSize) 
      let $l1 := $leptons[$l1Idx]
      let $l2 := $leptons[$l2Idx]
      where $l1.type = $l2.type and $l1.charge != $l2.charge
      let $mass := abs(91.2 - hep:AddPtEtaPhiM2($l1, $l2).mass)
      order by $mass
      count $c 
      where $c <= 1
      return {"i": $l1Idx, "j": $l2Idx}
  )
  where exists($bestPair)

  let $maxPT := max(
    for $l in $leptons
    count $c 
    where $c != $bestPair.i and $c != $bestPair.j
    return $l.pt
  )

  return $maxPT
)

return hep:buildHistogram($filtered, $histogram)
