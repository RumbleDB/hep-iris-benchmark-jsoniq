import module namespace hep = "../common/hep.jq";
import module namespace hep-i = "../common/hep-i.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

declare function ConcatLeptons($event) {
  let $nLepton := $event.nMuon + $event.nElectron
  let $pt := ($event.Muon_pt[], $event.Electron_pt[])
  let $eta := ($event.Muon_eta[], $event.Electron_eta[])
  let $phi := ($event.Muon_phi[], $event.Electron_phi[])
  let $mass := ($event.Muon_mass[], $event.Electron_mass[])
  let $charge := ($event.Muon_charge[], $event.Electron_charge[])

  let $m := for $i in (1 to size($event.Muon_pt)) return "m"
  let $e := for $i in (1 to size($event.Electron_pt)) return "e"

  let $type := ($m, $e)

  return {
    "nLepton": $nLepton, "type": $type,
    "pt": $pt, "eta": $eta, "phi": $phi, "mass": $mass, "charge": $charge
  }
};

let $histogram := hep:histogramConsts(15, 250, 100)

let $filtered := (
  for $i in parquet-file($dataPath)
  where ($i.nMuon + $i.nElectron) > 2
  let $leptons := ConcatLeptons($i)

  let $pairs := (
    for $iIdx in (1 to (size($leptons.pt) - 1))
    return
      for $jIdx in (($iIdx + 1) to size($leptons.pt))
      where $leptons.type[[$iIdx]] = $leptons.type[[$jIdx]] and
        $leptons.charge[[$iIdx]] != $leptons.charge[[$jIdx]]
      let $particleOne := hep-i:MakeParticle($leptons, $iIdx)
      let $particleTwo := hep-i:MakeParticle($leptons, $jIdx)
      return {
        "i": $iIdx, "j": $jIdx,
        "mass": abs(91.2 - hep:AddPtEtaPhiM2($particleOne, $particleTwo).mass)
      }
  )
  where exists($pairs)

  let $minMass := min($pairs.mass)
  let $minPair := (
    for $j in $pairs
    where $j.mass = $minMass
    return $j
  )

  let $maxOtherPt := max(
    for $j in (1 to size($leptons.pt))
    where $j != $minPair.i and $j != $minPair.j
    return $leptons.pt[[$j]]
  )

  let $otherLeptonMass := (
    for $j in (1 to size($leptons.pt))
    where $j != $minPair.i and $j != $minPair.j and
      $leptons.pt[[$j]] = $maxOtherPt
    let $transverseMass := 2 * $i.MET_pt * $maxOtherPt *
      (1.0 - cos(hep:DeltaPhi($i.MET_phi, $leptons.phi[[$j]])))
    return $transverseMass
  )

  return $otherLeptonMass
)

return hep:buildHistogram($filtered, $histogram)
