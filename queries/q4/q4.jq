let $bucketWidth := (2000 - 0) div 100.0
let $bucketCenter := $bucketWidth div 2
let $filtered := (
    for $i in parquet-file("/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet")
        let $subCount := count(
            for $j in size($i.Jet_pt)
            where $i.Jet_pt[[$j]] gt 40 and abs($i.Jet_eta[[$j]]) lt 1
            return $i.Jet_pt[[$j]])
        where $subCount gt 1
        return $i.MET_sumet)
let $res := (
    for $sumet in $filtered
        let $f := 
            if ($sumet lt 0) then 0
            else
                if ($sumet lt 2000) then $sumet
                else 2000
        let $centered := ($f - $bucketCenter) div $bucketWidth
        let $rounded := round($centered)
        let $x := $rounded * $bucketWidth + $bucketCenter
        group by $x
        order by $x
        return {"x": $x, "y": count($x)})
for $j in $res
return $j
