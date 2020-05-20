declare function histogramConsts($loBound, $hiBound, $binCount) {
  let $bucketWidth := ($hiBound - $loBound) div $binCount
  let $bucketCenter := $bucketWidth div 2

  let $loConst := round(($loBound - $bucketCenter) div $bucketWidth)
  let $hiConst := round(($hiBound - $bucketCenter) div $bucketWidth)

  return {"bins": $binCount, "width": $bucketWidth, "center": $bucketCenter, "loConst": $loConst, "hiConst": $hiConst,
          "loBound": $loBound, "hiBound": $hiBound}
};

declare function computeInvariantMass($event, $particleOneIdx, $particleTwoIdx) {
	let $eta_diff := $event.Muon_eta[[$particleOneIdx]] - $event.Muon_eta[[$particleTwoIdx]]
	let $phi_diff := $event.Muon_phi[[$particleOneIdx]] - $event.Muon_phi[[$particleTwoIdx]]
	let $cosh := (exp($eta_diff) + exp(-$eta_diff)) div 2
	let $invariant_mass := 2 * $event.Muon_pt[[$particleOneIdx]] * $event.Muon_pt[[$particleTwoIdx]] * ($cosh - cos($phi_diff))
	return $invariant_mass
};

let $dataPath := "/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet"
let $histogram := histogramConsts(0, 2000, 100)


let $filtered := (
	for $i in parquet-file($dataPath)
	where $i.nMuon > 1
	let $pairs := (
		for $iIdx in (1 to (size($i.Muon_charge) - 1))
		return	for $jIdx in (($iIdx + 1) to size($i.Muon_charge))
				where $i.Muon_charge[[$iIdx]] != $i.Muon_charge[[$jIdx]]
				return [$iIdx, $jIdx] 
	)
	where exists($pairs)

	let $temp := (
		for $pair in $pairs
		let $invariantMass := computeInvariantMass($i, $pair[[1]], $pair[[2]])
		where 60 < $invariantMass and $invariantMass < 120
		return $invariantMass
	) 
	where exists($temp)

	return $i.MET_sumet
)


for $i in $filtered
let $y := if ($i < $histogram.loBound) 
          then $histogram.loConst
          else
              if ($i < $histogram.hiBound)
              then round(($i - $histogram.center) div $histogram.width)
              else $histogram.hiConst
let $x := $y * $histogram.width + $histogram.center
group by $x
order by $x
return {"x": $x, "y": count($i)}