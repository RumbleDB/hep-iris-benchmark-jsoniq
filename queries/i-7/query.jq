import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $histogram := hep:histogramConsts(15, 200, 100)


let $filtered := (
	for $i in parquet-file($dataPath)
	let $filteredJets := (
		for $jetIdx in (1 to size($i.Jet_pt))
		where $i.Jet_pt[[$jetIdx]] > 30
			
		let $filteredElectrons := (
			for $electronIdx in (1 to size($i.Electron_pt))
				
			let $deltaR := hep-i:DeltaR(
				$i.Jet_phi[[$jetIdx]], 
				$i.Electron_phi[[$electronIdx]], 
				$i.Jet_eta[[$jetIdx]], 
				$i.Electron_eta[[$electronIdx]])

			where $i.Electron_pt[[$electronIdx]] > 10 and $deltaR < 40
			return $electronIdx
		)
		where empty($filteredElectrons)

		let $filteredMuons := (
			for $muonIdx in (1 to size($i.Muon_pt))

			let $deltaR := hep-i:DeltaR(
				$i.Jet_phi[[$jetIdx]], 
				$i.Muon_phi[[$muonIdx]], 
				$i.Jet_eta[[$jetIdx]], 
				$i.Muon_eta[[$muonIdx]])

			where $i.Muon_pt[[$muonIdx]] > 10 and $deltaR < 40
			return $muonIdx
		)
		where empty($filteredMuons)
			
		return $i.Jet_pt[[$jetIdx]]
	)
	where exists($filteredJets)
	
	return sum($filteredJets)
)


return hep:buildHistogram($filtered, $histogram)
