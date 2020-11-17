import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in parquet-file($dataPath)
  where ($event.nMuon + $event.nElectron) > 2
  let $leptons := hep-i:ConcatLeptons($event)

  let $closest-lepton-pair := (
    for $i in (1 to (size($leptons.pt) - 1))
    for $j in (($i + 1) to size($leptons.pt))
    where $leptons.type[[$i]] = $leptons.type[[$j]] and
      $leptons.charge[[$i]] != $leptons.charge[[$j]]
    let $particleOne := hep-i:MakeParticle($leptons, $i)
    let $particleTwo := hep-i:MakeParticle($leptons, $j)
    let $mass := hep:AddPtEtaPhiM2($particleOne, $particleTwo).mass
    order by abs(91.2 - $mass) ascending
    return {"i": $i, "j": $j}
  )[1]
  where exists($closest-lepton-pair)

  let $leading-other-lepton-idx := (
    for $i in (1 to size($leptons.pt))
    where $i != $closest-lepton-pair.i and $i != $closest-lepton-pair.j
    order by $leptons.pt[[$i]] descending
    return $i
  )[1]

  let $other-lepton-pt := $leptons.pt[[$leading-other-lepton-idx]]
  let $other-lepton-phi := $leptons.phi[[$leading-other-lepton-idx]]
  return 2 * $event.MET_pt * $other-lepton-pt *
    (1.0 - cos(hep:DeltaPhi($event.MET_phi, $other-lepton-phi)))
)

return hep:histogram($filtered, 15, 250, 100)
