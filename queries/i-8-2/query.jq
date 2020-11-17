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

  let $m := for $m in (1 to size($event.Muon_pt)) return "m"
  let $e := for $e in (1 to size($event.Electron_pt)) return "e"

  let $type := ($m, $e)

  return {
    "nLepton": $nLepton, "type": $type,
    "pt": $pt, "eta": $eta, "phi": $phi, "mass": $mass, "charge": $charge
  }
};

let $histogram := hep:histogramConsts(15, 60, 100)

let $filtered := (
  for $event in parquet-file($dataPath)
  where ($event.nMuon + $event.nElectron) > 2
  let $leptons := ConcatLeptons($event)

  let $pairs := (
    for $i in (1 to (size($leptons.pt) - 1))
    for $j in (($i + 1) to size($leptons.pt))
    where $leptons.type[[$i]] = $leptons.type[[$j]] and
      $leptons.charge[[$i]] != $leptons.charge[[$j]]
    let $particleOne := hep-i:MakeParticle($leptons, $i)
    let $particleTwo := hep-i:MakeParticle($leptons, $j)
    return {
      "i": $i, "j": $j,
      "mass": abs(91.2 - hep:AddPtEtaPhiM2($particleOne, $particleTwo).mass)
    }
  )
  where exists($pairs)

  let $minMass := min($pairs.mass)
  let $minPair := (
    for $pair in $pairs
    where $pair.mass = $minMass
    return $pair
  )

  let $maxOtherPt := max(
    for $i in (1 to size($leptons.pt))
    where $i != $minPair.i and $i != $minPair.j
    return $leptons.pt[[$i]]
  )

  return $maxOtherPt
)

return hep:buildHistogram($filtered, $histogram)
