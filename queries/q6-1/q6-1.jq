declare function buildHistogram($rawData, $histoConsts) {
  for $i in $rawData
  let $y := if ($i < $histoConsts.loBound) 
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

declare function MakeMuons($event) {
  for $i in (1 to integer($event.nMuon))
  return {
      "idx": $i,
      "pt": $event.Muon_pt[[$i]],
      "eta": $event.Muon_eta[[$i]],
      "phi": $event.Muon_phi[[$i]],
      "mass": $event.Muon_mass[[$i]],
      "charge": $event.Muon_charge[[$i]],
      "pfRelIso03_all": $event.Muon_pfRelIso03_all[[$i]],
      "pfRelIso04_all": $event.Muon_pfRelIso04_all[[$i]],
      "tightId": $event.Muon_tightId[[$i]],
      "softId": $event.Muon_softId[[$i]],
      "dxy": $event.Muon_dxy[[$i]],
      "dxyErr": $event.Muon_dxyErr[[$i]],
      "dz": $event.Muon_dz[[$i]],
      "dzErr": $event.Muon_dzErr[[$i]],
      "jetIdx": $event.Muon_jetIdx[[$i]],
      "genPartIdx": $event.Muon_genPartIdx[[$i]]
  }
};

declare function MakeElectrons($event) {
  for $i in (1 to integer($event.nElectron))
  return {
      "idx": $i,
      "pt": $event.Electron_pt[[$i]],
      "eta": $event.Electron_eta[[$i]],
      "phi": $event.Electron_phi[[$i]],
      "mass": $event.Electron_mass[[$i]],
      "pfRelIso03_all": $event.Electron_pfRelIso03_all[[$i]],
      "dxy": $event.Electron_dxy[[$i]],
      "dxyErr": $event.Electron_dxyErr[[$i]],
      "dz": $event.Electron_dz[[$i]],
      "dzErr": $event.Electron_dzErr[[$i]],
      "cutBasedId": $event.Electron_cutBasedId[[$i]],
      "pfId": $event.Electron_pfId[[$i]],
      "jetIdx": $event.Electron_jetIdx[[$i]],
      "genPartIdx": $event.Electron_genPartIdx[[$i]]
  }
};

declare function MakeJet($event) {
  for $i in (1 to integer($event.nJet))
  return {
      "idx": $i,
      "pt": $event.Jet_pt[[$i]],
      "eta": $event.Jet_eta[[$i]],
      "phi": $event.Jet_phi[[$i]],
      "mass": $event.Jet_mass[[$i]],
      "puId": $event.Jet_puId[[$i]],
      "btag": $event.Jet_btag[[$i]]
  }
};

declare function RestructureEvent($event) {
  let $muonList := MakeMuons($event)
  let $electronList := MakeElectrons($event)
  let $jetList := MakeJet($event)
  return {| $event, {"muons": $muonList, "electrons": $electronList, "jets": $jetList} |}
};

declare function RestructureData($data) {
  for $event in $data
  return RestructureEvent($event)
};

declare function RestructureDataParquet($path) {
  for $event in parquet-file($path)
  return RestructureEvent($event)
};

let $dataPath := "/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet"
let $histogram := histogramConsts(15, 40, 100)


let $filtered := (
  for $i in RestructureDataParquet($dataPath)
  where $i.nJet > 2

  let $triplets := (
    for $j1 in $i.jets[]
      for $j2 in $i.jets[]
        for $j3 in $i.jets[]
        where $j1.idx < $j2.idx and $j2.idx < $j3.idx 
        let $triJetMass := abs(172.5 - TriJet($j1, $j2, $j3).mass)
        order by $triJetMass
        count $c 
        where $c <= 1
        return ($j1.pt, $j2.pt, $j3.pt)
  )

  return $triplets
)


return buildHistogram($filtered, $histogram)