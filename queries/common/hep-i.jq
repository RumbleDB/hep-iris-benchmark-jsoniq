module namespace hep-i = "hep-i.jq";
import module namespace hep = "../common/hep.jq";

declare function hep-i:computeInvariantMass($event, $particleOneIdx, $particleTwoIdx) {
	let $eta_diff := $event.Muon_eta[[$particleOneIdx]] - $event.Muon_eta[[$particleTwoIdx]]
	let $phi_diff := $event.Muon_phi[[$particleOneIdx]] - $event.Muon_phi[[$particleTwoIdx]]
	let $cosh := (exp($eta_diff) + exp(-$eta_diff)) div 2
	let $invariant_mass := 2 * $event.Muon_pt[[$particleOneIdx]] * $event.Muon_pt[[$particleTwoIdx]] * ($cosh - cos($phi_diff))
	return $invariant_mass
};

declare function hep-i:MakeJetParticle($event, $jetIdx) {
	{"pt": $event.Jet_pt[[$jetIdx]], "eta": $event.Jet_eta[[$jetIdx]], "phi": $event.Jet_phi[[$jetIdx]], 
	"mass": $event.Jet_mass[[$jetIdx]]}
};

declare function hep-i:MakeParticle($event, $idx) {
       {"pt": $event.pt[[$idx]], "eta": $event.eta[[$idx]], "phi": $event.phi[[$idx]], "mass": $event.mass[[$idx]]}
};

declare function hep-i:DeltaR($phi1, $phi2, $eta1, $eta2) {
       let $deltaEta := $eta1 - $eta2
       let $deltaPhi := hep:DeltaPhi($phi1, $phi2)
       return sqrt($deltaPhi * $deltaPhi + $deltaEta * $deltaEta)
};
