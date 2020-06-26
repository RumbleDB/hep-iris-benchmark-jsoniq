declare function buildHistogram($rawData, $histoConsts) {
  for $i in $rawData
  let $y :=     if ($i < $histoConsts.loBound) 
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
    "charge": $event.Electron_charge[[$i]],
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

declare function sinh($x) {
  (exp($x) - exp(-$x)) div 2.0
};

declare function cosh($x) {
  (exp($x) + exp(-$x)) div 2.0
};

declare function PtEtaPhiM2PxPyPzE($vect) {
  let $x := $vect.pt * cos($vect.phi)
  let $y := $vect.pt * sin($vect.phi)
  let $z := $vect.pt * sinh($vect.eta)
  let $temp := $vect.pt * cosh($vect.eta)
  let $e := $temp * $temp + $vect.mass * $vect.mass
  return {"x": $x, "y": $y, "z": $z, "e": $e}
};

declare function AddPxPyPzE2($particleOne, $particleTwo) {
  let $x := $particleOne.x + $particleTwo.x
  let $y := $particleOne.y + $particleTwo.y
  let $z := $particleOne.z + $particleTwo.z
  let $e := $particleOne.e + $particleTwo.e
  return {"x": $x, "y": $y, "z": $z, "e": $e}
};

declare function RhoZ2Eta($rho, $z) {
  let $temp := $z div $rho
  return log($temp + sqrt($temp * $temp + 1.0))
};

declare function PxPyPzE2PtEtaPhiM($particle) {
  let $sqX := $particle.x * $particle.x
  let $sqY := $particle.y * $particle.y
  let $sqZ := $particle.z * $particle.z
  let $sqE := $particle.e * $particle.e

  let $pt := sqrt($sqX + $sqY)
  let $eta := RhoZ2Eta($pt, $particle.z)
  let $phi := if ($particle.x = 0.0 and $particle.y = 0.0)
        then 0.0
        else atan2($particle.y, $particle.x)
  let $mass := sqrt($sqE - $sqZ - $sqY - $sqX)

  return {"pt": $pt, "eta": $eta, "phi": $phi, "mass": $mass}
};

declare function AddPtEtaPhiM2($particleOne, $particleTwo) {
  PxPyPzE2PtEtaPhiM(
    AddPxPyPzE2(
      PtEtaPhiM2PxPyPzE($particleOne),
      PtEtaPhiM2PxPyPzE($particleTwo)
      )
    )
};

declare function DeltaPhi($phi1, $phi2) {
  ($phi1 - $phi2 + pi()) mod (2 * pi()) - pi()
};

declare function ConcatLeptons($event) {
  let $muons := (
    for $i in $event.muons[]
    return {| $i, {"type": "m"}  |}
  )  

  let $electrons := (
    for $i in $event.electrons[]
    return {| $i, {"type": "e"}  |}
  )  

  return ($muons, $electrons)
};

let $dataPath := "/home/dan/data/garbage/git/rumble-root-queries/rumble/data/Run2012B_SingleMu_small.parquet"
let $histogram := histogramConsts(15, 250, 100)


let $filtered := (
  for $i in RestructureDataParquet($dataPath)
  let $leptonSize := integer($i.nMuon + $i.nElectron)
  where $leptonSize > 2 

  let $leptons := ConcatLeptons($i)
  let $bestPair := (
    for $l1Idx in (1 to ($leptonSize - 1)) 
      for $l2Idx in (($l1Idx + 1) to $leptonSize) 
      let $l1 := $leptons[$l1Idx]
      let $l2 := $leptons[$l2Idx]
      where $l1.type = $l2.type and $l1.charge != $l2.charge
      let $mass := abs(91.2 - AddPtEtaPhiM2($l1, $l2).mass)
      order by $mass
      count $c 
      where $c <= 1
      return {"i": $l1Idx, "j": $l2Idx}
  )
  where exists($bestPair)

  let $otherL := (
    for $l in $leptons
    count $c 
    where $c != $bestPair.i and $c != $bestPair.j
    order by $l.pt descending
    count $d 
    where $d <= 1
    return $l
  )

  return 2 * $i.MET_pt * $otherL.pt * (1.0 - cos(DeltaPhi($i.MET_phi, $otherL.phi)))
)


return buildHistogram($filtered, $histogram)
