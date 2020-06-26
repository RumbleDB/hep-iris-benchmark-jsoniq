declare function buildHistogram($rawData, $histoConsts) {
    for $i in $rawData
    let $y :=     if ($i < $histoConsts.loBound) 
                  then $histoConsts.loConst
                  else
                    if ($i < $histoConsts.hiBound)
                    then round(($i - $histoConsts.center) div $histoConsts.width)
                    else $histoConsts.hiConst
    let $x := $y * $histoConsts.width + $histoConsts.center
    group by $x
    order by $x
    return {"x": $x, "y": count($y)}
};

declare function histogramConsts($loBound, $hiBound, $binCount) {
    let $bucketWidth := ($hiBound - $loBound) div $binCount
    let $bucketCenter := $bucketWidth div 2

    let $loConst := round(($loBound - $bucketCenter) div $bucketWidth)
    let $hiConst := round(($hiBound - $bucketCenter) div $bucketWidth)

    return {"bins": $binCount, "width": $bucketWidth, "center": $bucketCenter, "loConst": $loConst, "hiConst": $hiConst,
            "loBound": $loBound, "hiBound": $hiBound}
};

declare function DeltaPhi($phi1, $phi2) {
	($phi1 - $phi2 + pi()) mod (2 * pi()) - pi()
};

declare function DeltaR($phi1, $phi2, $eta1, $eta2) {
	let $deltaEta := $eta1 - $eta2
	let $deltaPhi := DeltaPhi($phi1, $phi2)
	return sqrt($deltaPhi * $deltaPhi + $deltaEta * $deltaEta)
};

let $dataPath := "/home/dan/data/garbage/git/rumble-root-queries/rumble/data/Run2012B_SingleMu_small.parquet"
let $histogram := histogramConsts(15, 200, 100)


let $filtered := (
	for $i in parquet-file($dataPath)
	let $filteredJets := (
		for $jetIdx in (1 to size($i.Jet_pt))
		where $i.Jet_pt[[$jetIdx]] > 30
			
		let $filteredElectrons := (
			for $electronIdx in (1 to size($i.Electron_pt))
				
			let $deltaR := DeltaR(
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

			let $deltaR := DeltaR(
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


return buildHistogram($filtered, $histogram)
