let $bucketWidth := (60 - 15) div 100.0
let $bucketCenter := 0.375
for $i in root-file("/mnt/scratch/muellein/root-rumble/Run2012B_SingleMu_small.root", "Events")
let $jetSum := sum($i.Jet_pt[])
let $filtered :=
    if ($jetSum lt 15) then 15
    else
        if ($jetSum lt 60) then $jetSum 
        else 60
let $centered := ($filtered - $bucketCenter) div $bucketWidth
let $rounded := round($centered)
let $x := $rounded * $bucketWidth + $bucketCenter
group by $x
order by $x
return {"x": $x, "y": count($i)}