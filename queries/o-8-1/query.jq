import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

declare function ConcatLeptons($event) {
  let $muons := (
    for $muon in $event.muons[]
    return {| $muon, {"type": "m"}  |}
  )

  let $electrons := (
    for $electron in $event.electrons[]
    return {| $electron, {"type": "e"}  |}
  )

  return ($muons, $electrons)
};

let $filtered := (
  for $event in hep:RestructureDataParquet($dataPath)
  let $leptonSize := integer($event.nMuon + $event.nElectron)
  where $leptonSize > 2

  let $leptons := ConcatLeptons($event)
  let $bestPair := (
    for $i in (1 to ($leptonSize - 1))
    for $j in (($i + 1) to $leptonSize)
    let $lepton1 := $leptons[$i]
    let $lepton2 := $leptons[$j]
    where $lepton1.type = $lepton2.type and $lepton1.charge != $lepton2.charge
    let $mass := abs(91.2 - hep:AddPtEtaPhiM2($lepton1, $lepton2).mass)
    order by $mass
    count $c
    where $c <= 1
    return {"i": $i, "j": $j}
  )
  where exists($bestPair)

  let $otherL := (
    for $lepton in $leptons
    count $c
    where $c != $bestPair.i and $c != $bestPair.j
    order by $lepton.pt descending
    count $d
    where $d <= 1
    return $lepton
  )

  return 2 * $event.MET_pt * $otherL.pt * (1.0 - cos(hep:DeltaPhi($event.MET_phi, $otherL.phi)))
)

return hep:histogram($filtered, 15, 250, 100)
