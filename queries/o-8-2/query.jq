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
  where integer($event.nMuon + $event.nElectron) > 2

  let $leptons := ConcatLeptons($event)
  let $closest-lepton-pair := (
    for $lepton1 at $i in $leptons
    for $lepton2 at $j in $leptons
    where $i < $j
    where $lepton1.type = $lepton2.type and $lepton1.charge != $lepton2.charge
    order by abs(91.2 - hep:AddPtEtaPhiM2($lepton1, $lepton2).mass) ascending
    return {"i": $i, "j": $j}
  )[1]
  where exists($closest-lepton-pair)

  return max(
    for $lepton at $i in $leptons
    where $i != $closest-lepton-pair.i and $i != $closest-lepton-pair.j
    return $lepton.pt
  )
)

return hep:histogram($filtered, 15, 60, 100)
