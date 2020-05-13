let $bucketWidth := (60 - 15) div 100.0
let $bucketCenter := 0.375

let $loConst := round((15 - $bucketCenter) div $bucketWidth)
let $hiConst := round((60 - $bucketCenter) div $bucketWidth)

let $temp := (
	for $i in parquet-file("/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet")
	let $filtered := (
		for $jet in $i.Jet_pt[]
		return 	if ($jet < 15) then $loConst
			    else
			        if ($jet < 60) then round(($jet - $bucketCenter) div $bucketWidth)
			        else $hiConst)
	return $filtered
)

for $i in $temp
let $x := $i * $bucketWidth + $bucketCenter
group by $x
order by $x
return {"x": $x, "y": count($i)}