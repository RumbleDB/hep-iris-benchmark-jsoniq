let $bucketWidth := (1 - 0) div 100.0
let $bucketCenter := $bucketWidth div 2

let $loConst := round(-$bucketCenter div $bucketWidth)
let $hiConst := round((1 - $bucketCenter) div $bucketWidth)

let $filtered := (
	for $i in parquet-file("/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet")
		where $i.nJet > 2
		let $triplets := (
				for $iIdx in (1 to size($i.Jet_pt))
					return for $jIdx in (($iIdx + 1) to size($i.Jet_pt))
								return for $kIdx in (($jIdx + 1) to size($i.Jet_pt))
									return {"idx": [$iIdx, $jIdx, $kIdx], "mass": abs(172.5 - $i.Jet_mass[[$iIdx]] - $i.Jet_mass[[$jIdx]] - $i.Jet_mass[[$kIdx]])} 
				)

		let $minMass := min($triplets.mass)

		let $minTriplet := (
			for $j in $triplets
				where $j.mass = $minMass
				return $j
			)

		let $btag := (
			for $j in $minTriplet.idx[]
				return $i.Jet_btag[[$j]]
			)

		return max($btag)
	)

for $i in $filtered
    let $binned := 
        if ($i < 0) then $loConst
        else
            if ($i < 1) then round(($i - $bucketCenter) div $bucketWidth)
            else $hiConst
    let $x := $binned * $bucketWidth + $bucketCenter
    group by $x
    order by $x
    return {"x": $x, "y": count($i)}