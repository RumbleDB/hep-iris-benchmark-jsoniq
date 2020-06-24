module namespace hep = "hep.jq";
import module namespace math = "../common/math.jq";

declare function hep:buildHistogram($rawData, $histoConsts) {
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

declare function hep:histogramConsts($loBound, $hiBound, $binCount) {
  let $bucketWidth := ($hiBound - $loBound) div $binCount
  let $bucketCenter := $bucketWidth div 2

  let $loConst := round(($loBound - $bucketCenter) div $bucketWidth)
  let $hiConst := round(($hiBound - $bucketCenter) div $bucketWidth)

  return {
    "bins": $binCount,
    "width": $bucketWidth,
    "center": $bucketCenter,
    "loConst": $loConst,
    "hiConst": $hiConst,
    "loBound": $loBound,
    "hiBound": $hiBound
  }
};

declare function hep:MakeMuons($event) {
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

declare function hep:MakeElectrons($event) {
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

declare function hep:MakeJet($event) {
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

declare function hep:RestructureEvent($event) {
  let $muonList := hep:MakeMuons($event)
  let $electronList := hep:MakeElectrons($event)
  let $jetList := hep:MakeJet($event)
  return {| $event,
           {
              "muons": [ $muonList ],
              "electrons": [ $electronList ],
              "jets": [ $jetList ]
           }
         |}
};

declare function hep:RestructureData($data) {
  for $event in $data
  return hep:RestructureEvent($event)
};

declare function hep:RestructureDataParquet($path) {
  for $event in parquet-file($path)
  return hep:RestructureEvent($event)
};

declare function hep:computeInvariantMass($m1, $m2) {
  2 * $m1.pt * $m2.pt * (math:cosh($m1.eta - $m2.eta) - cos($m1.phi - $m2.phi))
};

declare function hep:PtEtaPhiM2PxPyPzE($vect) {
  let $x := $vect.pt * cos($vect.phi)
  let $y := $vect.pt * sin($vect.phi)
  let $z := $vect.pt * math:sinh($vect.eta)
  let $temp := $vect.pt * math:cosh($vect.eta)
  let $e := $temp * $temp + $vect.mass * $vect.mass
  return {"x": $x, "y": $y, "z": $z, "e": $e}
};

declare function hep:AddPxPyPzE2($particleOne, $particleTwo) {
  let $x := $particleOne.x + $particleTwo.x
  let $y := $particleOne.y + $particleTwo.y
  let $z := $particleOne.z + $particleTwo.z
  let $e := $particleOne.e + $particleTwo.e
  return {"x": $x, "y": $y, "z": $z, "e": $e}
};

declare function hep:AddPxPyPzE3($particleOne, $particleTwo, $particleThree) {
  let $x := $particleOne.x + $particleTwo.x + $particleThree.x
  let $y := $particleOne.y + $particleTwo.y + $particleThree.y
  let $z := $particleOne.z + $particleTwo.z + $particleThree.z
  let $e := $particleOne.e + $particleTwo.e + $particleThree.e
  return {"x": $x, "y": $y, "z": $z, "e": $e}
};

declare function hep:RhoZ2Eta($rho, $z) {
  let $temp := $z div $rho
  return log($temp + sqrt($temp * $temp + 1.0))
};

declare function hep:PxPyPzE2PtEtaPhiM($particle) {
  let $sqX := $particle.x * $particle.x
  let $sqY := $particle.y * $particle.y
  let $sqZ := $particle.z * $particle.z
  let $sqE := $particle.e * $particle.e

  let $pt := sqrt($sqX + $sqY)
  let $eta := hep:RhoZ2Eta($pt, $particle.z)
  let $phi := if ($particle.x = 0.0 and $particle.y = 0.0)
        then 0.0
        else atan2($particle.y, $particle.x)
  let $mass := sqrt($sqE - $sqZ - $sqY - $sqX)

  return {"pt": $pt, "eta": $eta, "phi": $phi, "mass": $mass}
};

declare function hep:TriJet($particleOne, $particleTwo, $particleThree) {
  hep:PxPyPzE2PtEtaPhiM(
    hep:AddPxPyPzE3(
      hep:PtEtaPhiM2PxPyPzE($particleOne),
      hep:PtEtaPhiM2PxPyPzE($particleTwo),
      hep:PtEtaPhiM2PxPyPzE($particleThree)
      )
    )
};

declare function hep:AddPtEtaPhiM2($particleOne, $particleTwo) {
  hep:PxPyPzE2PtEtaPhiM(
    hep:AddPxPyPzE2(
      hep:PtEtaPhiM2PxPyPzE($particleOne),
      hep:PtEtaPhiM2PxPyPzE($particleTwo)
      )
    )
};

declare function hep:DeltaPhi($phi1, $phi2) {
  ($phi1 - $phi2 + pi()) mod (2 * pi()) - pi()
};

declare function hep:DeltaR($p1, $p2) {
  let $deltaEta := $p1.eta - $p2.eta
  let $deltaPhi := hep:DeltaPhi($p1.phi, $p2.phi)
  return sqrt($deltaPhi * $deltaPhi + $deltaEta * $deltaEta)
};
