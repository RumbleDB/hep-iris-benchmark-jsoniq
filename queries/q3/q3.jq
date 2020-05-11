let $bucketWidth := (60 - 15) div 100.0
let $bucketCenter := 0.375
for $i in root-file("/mnt/scratch/muellein/root-rumble/Run2012B_SingleMu_small.root", "Events")
    let $pointFiltered := sum(for $j in size($i.Jet_pt)
        where $i.Jet_eta[$j][] lt 1
        return $i.Jet_pt[$j][])
    let $filtered :=
        if ($pointFiltered lt 15) then 15
        else
            if ($pointFiltered lt 60) then $pointFiltered
            else 60
let $rounded := round(($filtered - $bucketCenter) div $bucketWidth)
let $x := $rounded * $bucketWidth + $bucketCenter
group by $x
order by $x
return {"x": $x, "y": count($rounded)}