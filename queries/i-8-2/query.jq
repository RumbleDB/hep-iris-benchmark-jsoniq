import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

declare function ConcatLeptons($event) {
  {
    "nLepton": $event.nMuon + $event.nElectron,
    "type": [for $m in (1 to size($event.Muon_pt)) return "m",
             for $e in (1 to size($event.Electron_pt)) return "e"],
    "pt":     [$event.Muon_pt[],     $event.Electron_pt[]],
    "eta":    [$event.Muon_eta[],    $event.Electron_eta[]],
    "phi":    [$event.Muon_phi[],    $event.Electron_phi[]],
    "mass":   [$event.Muon_mass[],   $event.Electron_mass[]],
    "charge": [$event.Muon_charge[], $event.Electron_charge[]]
  }
};

let $filtered := (
  for $event in parquet-file($dataPath)
  where ($event.nMuon + $event.nElectron) > 2
  let $leptons := ConcatLeptons($event)

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

  return max(
    for $i in (1 to size($leptons.pt))
    where $i != $closest-lepton-pair.i and $i != $closest-lepton-pair.j
    return $leptons.pt[[$i]]
  )
)

return hep:histogram($filtered, 15, 60, 100)
