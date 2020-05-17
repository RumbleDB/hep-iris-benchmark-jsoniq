declare function DeltaPhi($phi1, $phi2) {
	let $deltaPhi := $phi2 - $phi1
	return  if ($deltaPhi > pi()) then $deltaPhi - 2 * pi()
			else 	if ($deltaPhi <= -pi()) then $deltaPhi + 2 * pi()
				 	else $deltaPhi
};

declare function R($phi1, $phi2, $eta1, $eta2) {
	let $deltaPhi := DeltaPhi($phi1, $phi2)
	let $deltaEta := $eta2 - $eta1
	return $deltaPhi * $deltaPhi + $deltaEta * $deltaEta
};

declare function R2($phi1, $phi2, $eta1, $eta2) {
	let $R := R($phi1, $phi2, $eta1, $eta2)
	return sqrt($R)
};


let $bucketWidth := (200 - 15) div 100.0
let $bucketCenter := $bucketWidth div 2

let $loConst := round((15 - $bucketCenter) div $bucketWidth)
let $hiConst := round((200 - $bucketCenter) div $bucketWidth)



let $filtered := (
	for $i in parquet-file("/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet")
		let $filteredJets := (
			for $jetIdx in (1 to size($i.Jet_pt))
				where $i.Jet_pt[[$jetIdx]] > 30
				
				let $filteredElectrons := (
					for $electronIdx in (1 to size($i.Electron_pt))
					
					let $r2 := R2(
						$i.Jet_phi[[$jetIdx]], 
						$i.Electron_phi[[$electronIdx]], 
						$i.Jet_eta[[$jetIdx]], 
						$i.Electron_eta[[$electronIdx]])

					where $i.Electron_pt[[$electronIdx]] > 10 and $r2 < 40
					return $electronIdx
					)
				where empty($filteredElectrons)

				let $filteredMuons := (
					for $muonIdx in (1 to size($i.Muon_pt))
						where $i.Muon_pt[[$muonIdx]] > 10 and R2($i.Jet_phi[[$jetIdx]], $i.Muon_phi[[$muonIdx]], $i.Jet_eta[[$jetIdx]], $i.Muon_eta[[$muonIdx]]) < 40
						return $muonIdx
					)
				where empty($filteredMuons)
				
				return $i.Jet_pt[[$jetIdx]]
			)
		where exists($filteredJets)
		
		return sum($filteredJets)
	)

for $i in $filtered
    let $binned := 
        if ($i < 15) then $loConst
        else
            if ($i < 200) then round(($i - $bucketCenter) div $bucketWidth)
            else $hiConst
    let $x := $binned * $bucketWidth + $bucketCenter
    group by $x
    order by $x
    return {"x": $x, "y": count($i)}