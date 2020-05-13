let $bucketWidth := (2000 - 0) div 100.0
let $bucketCenter := $bucketWidth div 2

let $loConst := round(-$bucketCenter div $bucketWidth)
let $hiConst := round((2000 - $bucketCenter) div $bucketWidth)

let $filtered := (
	for $i in parquet-file("/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet")
		where $i.nMuon > 1
		let $pairs := (
			for $iIdx in (1 to size($i.Muon_charge))
				return for $jIdx in (($iIdx + 1) to size($i.Muon_charge))
					where $i.Muon_charge[[$iIdx]] != $i.Muon_charge[[$jIdx]]
					return [$iIdx, $jIdx] 
			)
		where exists($pairs)

		let $temp := (
			for $pair in $pairs
				let $eta_diff := $i.Muon_eta[[ $pair[[1]] ]] - $i.Muon_eta[[ $pair[[2]] ]]
				let $phi_diff := $i.Muon_phi[[ $pair[[1]] ]] - $i.Muon_phi[[ $pair[[2]] ]]
				let $cosh := (exp($eta_diff) + exp(-$eta_diff)) div 2
				let $invariant_mass := 2 * $i.Muon_pt[[ $pair[[1]] ]] * $i.Muon_pt[[ $pair[[2]] ]] * ($cosh - cos($phi_diff))
				where $invariant_mass > 60 and $invariant_mass < 120
				return $invariant_mass
				) 
		where exists($temp)

		return $i.MET_sumet
	)

for $i in $filtered
    let $binned := 
        if ($i < 0) then $loConst
        else
            if ($i < 2000) then round(($i - $bucketCenter) div $bucketWidth)
            else $hiConst
    let $x := $binned * $bucketWidth + $bucketCenter
    group by $x
    order by $x
    return {"x": $x, "y": count($i)}