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

declare function MakeMuons($event) {
  for $i in (1 to size($event.Muon_pt))
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
  for $i in (1 to size($event.Electron_pt))
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
  for $i in (1 to size($event.Jet_pt))
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
  return {| $event, {"muons": [ $muonList ], "electrons": [ $electronList ], "jets": [ $jetList ]} |}
};

declare function RestructureData($data) {
  for $event in $data
  return RestructureEvent($event)
};

declare function RestructureDataParquet($path) {
  for $event in parquet-file($path)
  return RestructureEvent($event)
};

declare function histogramConsts($loBound, $hiBound, $binCount) {
  let $bucketWidth := ($hiBound - $loBound) div $binCount
  let $bucketCenter := $bucketWidth div 2

  let $loConst := round(($loBound - $bucketCenter) div $bucketWidth)
  let $hiConst := round(($hiBound - $bucketCenter) div $bucketWidth)

  return {"bins": $binCount, "width": $bucketWidth, "center": $bucketCenter, "loConst": $loConst, "hiConst": $hiConst,
          "loBound": $loBound, "hiBound": $hiBound}
};

declare function cosh($x) {
	(exp($x) + exp(-$x)) div 2
};

declare function computeInvariantMass($m1, $m2) {
	2 * $m1.pt * $m2.pt * (cosh($m1.eta - $m2.eta) - cos($m1.phi - $m2.phi))
};

let $dataPath := "/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet"
let $histogram := histogramConsts(0, 2000, 100)


let $filtered := (
	for $i in RestructureDataParquet($dataPath)
	where $i.nMuon > 1
	
	let $masses := (
		for $m1 in $i.muons[]
			for $m2 in $i.muons[]
			where $m1.idx < $m2.idx 
			let $invariantMass := computeInvariantMass($m1, $m2)
			where 60 < $invariantMass and $invariantMass < 120
			return $invariantMass 
	)
	where exists($masses)

	return $i.MET_sumet
)


return buildHistogram($filtered, $histogram)