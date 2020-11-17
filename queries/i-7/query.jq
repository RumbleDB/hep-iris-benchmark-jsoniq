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

  for $i in (1 to size($event.Jet_pt))
  where $event.Jet_pt[[$i]] > 30

  let $leptons := ConcatLeptons($event)

  where empty(
    for $j in (1 to size($leptons.pt))
    let $deltaR := hep-i:DeltaR(
      $event.Jet_phi[[$i]], $leptons.phi[[$j]],
      $event.Jet_eta[[$i]], $leptons.eta[[$j]])
    where $leptons.pt[[$j]] > 10 and $deltaR < 40
    return {}
  )

  return sum($event.Jet_pt[[$i]])
)

return hep:histogram($filtered, 15, 200, 100)
