declare variable $inputPath as string? external;
let $bucketWidth := (2000 - 0) div 100.0
let $bucketCenter := $bucketWidth div 2
for $i in parquet-file("/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet")
let $filtered := 
    if ($i.MET_sumet lt 0) then 0
    else
        if ($i.MET_sumet lt 2000) then $i.MET_sumet
        else 2000
let $centered := ($filtered - $bucketCenter) div $bucketWidth
let $rounded := round($centered)
let $x := $rounded * $bucketWidth + $bucketCenter
group by $x
order by $x
return {"x": $x, "y": count($i.MET_sumet)}