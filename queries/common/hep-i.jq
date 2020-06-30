module namespace hep-i = "hep-i.jq";
import module namespace hep = "../common/hep.jq";

declare function hep-i:computeInvariantMass($event, $i, $j) {
  let $eta_diff := $event.Muon_eta[[$i]] - $event.Muon_eta[[$j]]
  let $phi_diff := $event.Muon_phi[[$i]] - $event.Muon_phi[[$j]]
  let $cosh := (exp($eta_diff) + exp(-$eta_diff)) div 2
  let $invariant_mass :=
    2 * $event.Muon_pt[[$i]] * $event.Muon_pt[[$j]] * ($cosh - cos($phi_diff))
  return $invariant_mass
};

declare function hep-i:MakeJetParticle($event, $i) {
  {
    "pt": $event.Jet_pt[[$i]],
    "eta": $event.Jet_eta[[$i]],
    "phi": $event.Jet_phi[[$i]],
    "mass": $event.Jet_mass[[$i]]
  }
};

declare function hep-i:MakeParticle($event, $i) {
  {
    "pt": $event.pt[[$i]],
    "eta": $event.eta[[$i]],
    "phi": $event.phi[[$i]],
    "mass": $event.mass[[$i]]
  }
};

declare function hep-i:DeltaR($phi1, $phi2, $eta1, $eta2) {
  let $deltaEta := $eta1 - $eta2
  let $deltaPhi := hep:DeltaPhi($phi1, $phi2)
  return sqrt($deltaPhi * $deltaPhi + $deltaEta * $deltaEta)
};
