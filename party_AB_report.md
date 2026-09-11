# Party components and coalition inversions

## Scope and definitions

The current sample contains 2 inverted cabinet party sets, 6 minimal connected (k=0) ideological inversions and 84 at-most-one-gap (k=1) minimal inversions. This standalone diagnostic retains its original **all-party ideological sensitivity**, including zero-seat parties. The manuscript's primary seat-winning ideological baseline is generated separately and is unchanged.

Cabinet history comes from the pinned contemporaneous-affiliation release. There are 35 distinct election-year cabinet party sets, observed on 4,096 dates (3,996 established and 100 provisional). The inverted sets occupy 260 days. UNKNOWN historical affiliations remain UNKNOWN; provisional primary assumptions add no party. Set counts are unweighted. Actual intervals, evidence status and date-level sensitivities remain linked separately.

For each party, $q_i=S v_i/V$, $d_i=s_i-q_i$, $R_i=s_i/q_i$, $A_i=\sum_d(s_{id}-S_dv_{id}/V_d)$ and $B_i=\sum_d S_dv_{id}/V_d-Sv_i/V$. All components are in seats. The exact checks require $d_i=A_i+B_i$, $A_C=\sum_{i\in C}A_i$, $B_C=\sum_{i\in C}B_i$ and $d_C=A_C+B_C$. The denominator includes every valid party vote, and the national seat total remains 513.

These are descriptive accounting contributions. The 2014/2018 joint-list allocations and 2022 federation allocations are attributed ex post to parties; the components do not identify a party-specific causal effect. Ratios with a zero quota are unavailable.

## Validation and provenance

The maintained Julia decomposition supplies the complete district-party panel. This diagnostic independently sums integer district votes/seats with rational arithmetic, checks every selected member vector and deletion, and verifies domain-relative minimality against all winning proper subsets. All 19,837 all-party k=0/k=1 registry rows and 35 cabinet party sets passed. The maximum saved-accounting discrepancy is 1.14e-13; the maximum serialized closure discrepancy is 3.55e-15, against an absolute tolerance of 1e-10 and zero relative tolerance.

Input/code provenance SHA-256: `c3bd02ed0721bd0173dd8fdd8a569a10d2faaa564120366ab76d65dc47d7012a`. The manuscript source is preserved at SHA-256 `f7ae224c700895c2a90ca46472d424daa1789af5f5c26fe660af4efd87362cc8`.

## Party component sign profiles

| Election | Parties | A+ B+ | A+ B- | A- B+ | A- B- | Substantial offsets |
| --- | --- | --- | --- | --- | --- | --- |
| 2014 | 32 | 7 | 5 | 13 | 7 | 5 |
| 2018 | 35 | 8 | 4 | 17 | 6 | 9 |
| 2022 | 32 | 6 | 3 | 11 | 12 | 4 |


## Current focal coalitions

| Code | Election | Domain | Period/interval | Days | Vote % | Seats | A_C | B_C | d_C | Members |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 14-05 | 2014 | cabinet | [["2016-04-14","2016-04-19"]] | 5 | 46.5358 | 259 | 15.742 | 4.529 | 20.271 | PCdoB; PDT; PMDB; PR; PSD; PT; PTB |
| 22-01 | 2022 | cabinet | [["2023-01-01","2023-09-13"]] | 255 | 48.8880 | 263 | 13.196 | -0.992 | 12.204 | MDB; PCdoB; PDT; PSB; PSD; PSOL; PT; REDE; UNIÃO |
| K14a | 2014 | k=0 minimal ideological | PSB--PTN |  | 48.9216 | 260 | 10.509 | -1.476 | 9.032 | PSB; PPS; PV; PTB; PT DO B; SOLIDARIEDADE; PMN; PHS; PMDB; PSD; PSDB; PTN |
| K14b | 2014 | k=0 minimal ideological | PTB--PR |  | 47.5913 | 257 | 11.621 | 1.236 | 12.857 | PTB; PT DO B; SOLIDARIEDADE; PMN; PHS; PMDB; PSD; PSDB; PTN; PPL; PRTB; PROS; PRP; PR |
| K14c | 2014 | k=0 minimal ideological | PT DO B--PSDC |  | 48.9868 | 257 | 6.267 | -0.569 | 5.697 | PT DO B; SOLIDARIEDADE; PMN; PHS; PMDB; PSD; PSDB; PTN; PPL; PRTB; PROS; PRP; PR; PRB; PTC; PSDC |
| K14d | 2014 | k=0 minimal ideological | SOLIDARIEDADE--PSL |  | 48.9830 | 257 | 6.586 | -0.869 | 5.717 | SOLIDARIEDADE; PMN; PHS; PMDB; PSD; PSDB; PTN; PPL; PRTB; PROS; PRP; PR; PRB; PTC; PSDC; PSL |
| K22a | 2022 | k=0 minimal ideological | MDB--UNIÃO |  | 49.9822 | 265 | -1.292 | 9.884 | 8.591 | MDB; PMN; PSDB; PSD; PMB; PODE; PROS; PRTB; AGIR; PTB; PP; DC; REPUBLICANOS; PSC; UNIÃO |
| K22b | 2022 | k=0 minimal ideological | PP--PL |  | 45.3499 | 258 | 22.821 | 2.535 | 25.355 | PP; DC; REPUBLICANOS; PSC; UNIÃO; PATRIOTA; NOVO; PL |


### Gross component contributions

| Code | Component | Gross positive | Gross negative (signed) | Top two positive | Share | Top two negative | Share |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 14-05 | A | 17.682 | -1.940 | PR; PSD | 54.8% | PT; PCdoB | 92.0% |
| 14-05 | B | 6.567 | -2.038 | PMDB; PDT | 88.1% | PT; PR | 95.4% |
| 14-05 | d | 22.773 | -2.502 | PMDB; PSD | 55.7% | PT; PCdoB | 100.0% |
| 22-01 | A | 26.073 | -12.876 | UNIÃO; PT | 70.7% | PSB; PSOL | 69.5% |
| 22-01 | B | 4.225 | -5.217 | UNIÃO; PDT | 65.6% | PSOL; PT | 91.8% |
| 22-01 | d | 26.461 | -14.257 | UNIÃO; PT | 68.2% | PSOL; PSB | 81.6% |
| K14a | A | 15.619 | -5.111 | PSD; PTB | 55.9% | PT DO B; PV | 98.2% |
| K14a | B | 5.787 | -7.263 | PMDB; PMN | 79.2% | PSDB; PV | 85.0% |
| K14a | d | 19.612 | -10.579 | PMDB; PSD | 64.7% | PSDB; PT DO B | 72.9% |
| K14b | A | 19.521 | -7.901 | PR; PSD | 49.6% | PT DO B; PRTB | 70.0% |
| K14b | B | 7.282 | -6.046 | PMDB; PROS | 68.5% | PSDB; PR | 95.1% |
| K14b | d | 23.526 | -10.669 | PMDB; PSD | 53.9% | PSDB; PT DO B | 72.3% |
| K14c | A | 15.572 | -9.306 | PR; PSD | 62.2% | PT DO B; PRTB | 59.4% |
| K14c | B | 7.651 | -8.220 | PMDB; PROS | 65.2% | PSDB; PRB | 87.4% |
| K14c | d | 19.369 | -13.672 | PMDB; PSD | 65.5% | PSDB; PT DO B | 56.4% |
| K14d | A | 15.572 | -8.986 | PR; PSD | 62.2% | PSL; PRTB | 58.0% |
| K14d | B | 7.351 | -8.220 | PMDB; PROS | 67.9% | PSDB; PRB | 87.4% |
| K14d | d | 19.369 | -13.652 | PMDB; PSD | 65.5% | PSDB; PSL | 56.3% |
| K22a | A | 21.276 | -22.569 | UNIÃO; MDB | 68.0% | PTB; PODE | 51.4% |
| K22a | B | 10.817 | -0.934 | PP; REPUBLICANOS | 56.5% | PSDB; MDB | 100.0% |
| K22a | d | 29.495 | -20.904 | UNIÃO; PP | 58.8% | PTB; PODE | 50.7% |
| K22b | A | 32.874 | -10.054 | PL; UNIÃO | 86.5% | PSC; PATRIOTA | 67.1% |
| K22b | B | 8.493 | -5.959 | PP; REPUBLICANOS | 71.9% | PL; NOVO | 97.9% |
| K22b | d | 35.441 | -10.086 | PL; UNIÃO | 70.2% | NOVO; PATRIOTA | 64.5% |


### Complete member vectors

Every focal coalition and each strongest k=1 case has its complete member vector below. A party's components stay fixed within an election; only the membership selector changes.

**14-05: 2014 Cabinet 14-05**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| PCdoB | -0.568875870255 | 0.488519169996 | -0.080356700259 |
| PDT | -0.155637890391 | 1.563527404099 | 1.407889513709 |
| PMDB | 3.908695216248 | 4.224685645374 | 8.133380861622 |
| PR | 5.042838190179 | -0.738392961619 | 4.304445228559 |
| PSD | 4.645941310001 | -0.093209109983 | 4.552732200019 |
| PT | -1.215411226585 | -1.206312102570 | -2.421723329156 |
| PTB | 4.084472535551 | 0.290251847866 | 4.374724383417 |
| Total | 15.742022264748 | 4.529069893163 | 20.271092157910 |


**G14: 2014 PTB--PR, omitting PPL**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| PTB | 4.084472535551 | 0.290251847866 | 4.374724383417 |
| PT DO B | -3.614965224246 | 0.333629438646 | -3.281335785600 |
| SOLIDARIEDADE | 0.692805791833 | 0.134202623133 | 0.827008414966 |
| PMN | 0.173314050944 | 0.358135025807 | 0.531449076750 |
| PHS | -0.092075568339 | 0.122714725586 | 0.030639157247 |
| PMDB | 3.908695216248 | 4.224685645374 | 8.133380861622 |
| PSD | 4.645941310001 | -0.093209109983 | 4.552732200019 |
| PSDB | 0.580946490325 | -5.011331274299 | -4.430384783974 |
| PTN | 0.392078083960 | -0.202781355606 | 0.189296728355 |
| PRTB | -1.912338055490 | 0.519049341240 | -1.393288714250 |
| PROS | -0.181697501903 | 0.763564555665 | 0.581867053763 |
| PRP | -1.212942039433 | 0.393581216195 | -0.819360823237 |
| PR | 5.042838190179 | -0.738392961619 | 4.304445228559 |
| Total | 12.507073279630 | 1.094099718005 | 13.601172997635 |


**K14a: 2014 PSB--PTN**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| PSB | 0.649364120776 | 0.322956538650 | 0.972320659427 |
| PPS | 0.491727162739 | -0.796948352731 | -0.305221189992 |
| PV | -1.403520118331 | -1.158713901176 | -2.562234019508 |
| PTB | 4.084472535551 | 0.290251847866 | 4.374724383417 |
| PT DO B | -3.614965224246 | 0.333629438646 | -3.281335785600 |
| SOLIDARIEDADE | 0.692805791833 | 0.134202623133 | 0.827008414966 |
| PMN | 0.173314050944 | 0.358135025807 | 0.531449076750 |
| PHS | -0.092075568339 | 0.122714725586 | 0.030639157247 |
| PMDB | 3.908695216248 | 4.224685645374 | 8.133380861622 |
| PSD | 4.645941310001 | -0.093209109983 | 4.552732200019 |
| PSDB | 0.580946490325 | -5.011331274299 | -4.430384783974 |
| PTN | 0.392078083960 | -0.202781355606 | 0.189296728355 |
| Total | 10.508783851461 | -1.476408148733 | 9.032375702727 |


**K14b: 2014 PTB--PR**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| PTB | 4.084472535551 | 0.290251847866 | 4.374724383417 |
| PT DO B | -3.614965224246 | 0.333629438646 | -3.281335785600 |
| SOLIDARIEDADE | 0.692805791833 | 0.134202623133 | 0.827008414966 |
| PMN | 0.173314050944 | 0.358135025807 | 0.531449076750 |
| PHS | -0.092075568339 | 0.122714725586 | 0.030639157247 |
| PMDB | 3.908695216248 | 4.224685645374 | 8.133380861622 |
| PSD | 4.645941310001 | -0.093209109983 | 4.552732200019 |
| PSDB | 0.580946490325 | -5.011331274299 | -4.430384783974 |
| PTN | 0.392078083960 | -0.202781355606 | 0.189296728355 |
| PPL | -0.886535198259 | 0.142217613013 | -0.744317585245 |
| PRTB | -1.912338055490 | 0.519049341240 | -1.393288714250 |
| PROS | -0.181697501903 | 0.763564555665 | 0.581867053763 |
| PRP | -1.212942039433 | 0.393581216195 | -0.819360823237 |
| PR | 5.042838190179 | -0.738392961619 | 4.304445228559 |
| Total | 11.620538081372 | 1.236317331018 | 12.856855412390 |


**K14c: 2014 PT DO B--PSDC**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| PT DO B | -3.614965224246 | 0.333629438646 | -3.281335785600 |
| SOLIDARIEDADE | 0.692805791833 | 0.134202623133 | 0.827008414966 |
| PMN | 0.173314050944 | 0.358135025807 | 0.531449076750 |
| PHS | -0.092075568339 | 0.122714725586 | 0.030639157247 |
| PMDB | 3.908695216248 | 4.224685645374 | 8.133380861622 |
| PSD | 4.645941310001 | -0.093209109983 | 4.552732200019 |
| PSDB | 0.580946490325 | -5.011331274299 | -4.430384783974 |
| PTN | 0.392078083960 | -0.202781355606 | 0.189296728355 |
| PPL | -0.886535198259 | 0.142217613013 | -0.744317585245 |
| PRTB | -1.912338055490 | 0.519049341240 | -1.393288714250 |
| PROS | -0.181697501903 | 0.763564555665 | 0.581867053763 |
| PRP | -1.212942039433 | 0.393581216195 | -0.819360823237 |
| PR | 5.042838190179 | -0.738392961619 | 4.304445228559 |
| PRB | -0.141556843351 | -2.174415198618 | -2.315972041969 |
| PTC | 0.135648983177 | 0.082692239228 | 0.218341222405 |
| PSDC | -1.263574806074 | 0.576540582975 | -0.687034223100 |
| Total | 6.266582879572 | -0.569116893264 | 5.697465986308 |


**K14d: 2014 SOLIDARIEDADE--PSL**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| SOLIDARIEDADE | 0.692805791833 | 0.134202623133 | 0.827008414966 |
| PMN | 0.173314050944 | 0.358135025807 | 0.531449076750 |
| PHS | -0.092075568339 | 0.122714725586 | 0.030639157247 |
| PMDB | 3.908695216248 | 4.224685645374 | 8.133380861622 |
| PSD | 4.645941310001 | -0.093209109983 | 4.552732200019 |
| PSDB | 0.580946490325 | -5.011331274299 | -4.430384783974 |
| PTN | 0.392078083960 | -0.202781355606 | 0.189296728355 |
| PPL | -0.886535198259 | 0.142217613013 | -0.744317585245 |
| PRTB | -1.912338055490 | 0.519049341240 | -1.393288714250 |
| PROS | -0.181697501903 | 0.763564555665 | 0.581867053763 |
| PRP | -1.212942039433 | 0.393581216195 | -0.819360823237 |
| PR | 5.042838190179 | -0.738392961619 | 4.304445228559 |
| PRB | -0.141556843351 | -2.174415198618 | -2.315972041969 |
| PTC | 0.135648983177 | 0.082692239228 | 0.218341222405 |
| PSDC | -1.263574806074 | 0.576540582975 | -0.687034223100 |
| PSL | -3.295337650938 | 0.033956915780 | -3.261380735157 |
| Total | 6.586210452881 | -0.868789416129 | 5.717421036752 |


**G18: 2018 PCdoB--PODE, omitting PSDB**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| PCdoB | 1.315174185938 | 0.743619917998 | 2.058794103936 |
| PT | 4.883103752832 | -1.750292125320 | 3.132811627512 |
| PDT | 3.102565640910 | 1.081566086019 | 4.184131726929 |
| PSB | 3.579884957138 | 0.113923601199 | 3.693808558336 |
| REDE | -3.941560240069 | 0.677441317397 | -3.264118922672 |
| PPS | -0.015174568217 | -0.287495946850 | -0.302670515068 |
| PV | -4.009164029906 | -0.306411857913 | -4.315575887818 |
| PTB | -1.031889159498 | 0.472041803101 | -0.559847356397 |
| AVANTE | -3.174030012111 | 0.494922139752 | -2.679107872359 |
| SOLIDARIEDADE | 0.992919191739 | 1.810844417364 | 2.803763609103 |
| PMN | -0.648914910332 | 0.336666419809 | -0.312248490523 |
| PMB | -1.345812345371 | 0.153934347903 | -1.191877997468 |
| PHS | -2.192468149345 | 0.734619252408 | -1.457848896938 |
| MDB | 4.338282118829 | 1.262046016985 | 5.600328135814 |
| PSD | 4.263497081280 | 0.723106074967 | 4.986603156246 |
| PODE | -0.373774741201 | -0.337746617699 | -0.711521358900 |
| Total | 5.742638772615 | 5.922784847118 | 11.665423619734 |


**22-01: 2022 Cabinet 22-01**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| MDB | 5.119561127196 | -0.398405481587 | 4.721155645608 |
| PCdoB | 0.176281811359 | 0.409695123043 | 0.585976934402 |
| PDT | -1.974201173994 | 1.024385772773 | -0.949815401221 |
| PSB | -5.545916698920 | -0.028660756361 | -5.574577455281 |
| PSD | 2.351878855613 | 0.760788987922 | 3.112667843535 |
| PSOL | -3.402078828028 | -2.659696487685 | -6.061775315713 |
| PT | 9.068253408506 | -2.130298562931 | 6.937954845575 |
| REDE | -1.954193441637 | 0.283381415393 | -1.670812026245 |
| UNIÃO | 9.356662317792 | 1.746901265966 | 11.103563583758 |
| Total | 13.196247377886 | -0.991908723467 | 12.204338654419 |


**G22: 2022 PP--PL, omitting DC**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| PP | 3.074500419422 | 3.167580238480 | 6.242080657902 |
| REPUBLICANOS | 1.373553456960 | 2.941742767707 | 4.315296224667 |
| PSC | -3.712380545199 | 0.594495265440 | -3.117885279759 |
| UNIÃO | 9.356662317792 | 1.746901265966 | 11.103563583758 |
| PATRIOTA | -3.034014858888 | -0.123514630067 | -3.157529488955 |
| NOVO | -2.806747452300 | -0.545199530334 | -3.351946982634 |
| PL | 19.069539963730 | -5.289798809648 | 13.779741154081 |
| Total | 23.321113301517 | 2.492206567543 | 25.813319869060 |


**K22a: 2022 MDB--UNIÃO**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| MDB | 5.119561127196 | -0.398405481587 | 4.721155645608 |
| PMN | -1.333037701850 | 0.130037118047 | -1.203000583804 |
| PSDB | -1.979493123939 | -0.535486004598 | -2.514979128537 |
| PSD | 2.351878855613 | 0.760788987922 | 3.112667843535 |
| PMB | -0.420997893327 | 0.031583315741 | -0.389414577586 |
| PODE | -5.660581199864 | 0.724922231686 | -4.935658968178 |
| PROS | -1.025547842826 | 0.276229312613 | -0.749318530213 |
| PRTB | -1.243372560319 | 0.123506163201 | -1.119866397118 |
| AGIR | -0.747500861032 | 0.002626800326 | -0.744874060706 |
| PTB | -5.944989995327 | 0.274694075375 | -5.670295919952 |
| PP | 3.074500419422 | 3.167580238480 | 6.242080657902 |
| DC | -0.500607707185 | 0.042335836403 | -0.458271870782 |
| REPUBLICANOS | 1.373553456960 | 2.941742767707 | 4.315296224667 |
| PSC | -3.712380545199 | 0.594495265440 | -3.117885279759 |
| UNIÃO | 9.356662317792 | 1.746901265966 | 11.103563583758 |
| Total | -1.292353253885 | 9.883551892723 | 8.591198638837 |


**K22b: 2022 PP--PL**

| Party | A_i | B_i | d_i |
| --- | --- | --- | --- |
| PP | 3.074500419422 | 3.167580238480 | 6.242080657902 |
| DC | -0.500607707185 | 0.042335836403 | -0.458271870782 |
| REPUBLICANOS | 1.373553456960 | 2.941742767707 | 4.315296224667 |
| PSC | -3.712380545199 | 0.594495265440 | -3.117885279759 |
| UNIÃO | 9.356662317792 | 1.746901265966 | 11.103563583758 |
| PATRIOTA | -3.034014858888 | -0.123514630067 | -3.157529488955 |
| NOVO | -2.806747452300 | -0.545199530334 | -3.351946982634 |
| PL | 19.069539963730 | -5.289798809648 | 13.779741154081 |
| Total | 22.820505594332 | 2.534542403947 | 25.355047998278 |


### Leave-one-party-out configurations

Every deletion is recomputed directly from district inputs. A deletion can leave the original ideological domain, so preservation of an inversion is not a contradiction of domain-relative minimality.

| Code | Deletions preserving inversion | Seat-pivotal members |
| --- | --- | --- |
| 14-05 | None | 7/7 |
| 22-01 | PCdoB, REDE | 7/9 |
| K14a | PT DO B, PMN | 10/12 |
| K14b | PPL | 13/14 |
| K14c | PPL | 15/16 |
| K14d | PPL | 15/16 |
| K22a | PMN, PMB, PROS, PRTB, AGIR, PTB, DC, PSC | 7/15 |
| K22b | DC | 7/8 |


## All-party one-gap sensitivity

The k=1 domain permits at most one missing interior party. Strongest follows the maintained criterion: lowest vote share within election, then fewer parties, then canonical coalition ID.

| Election | Cases | Reinforcement | Within-led offset | Between-led offset | A: median [min,max] | B: median [min,max] |
| --- | --- | --- | --- | --- | --- | --- |
| 2014 | 43 | 24 | 17 | 2 | 7.473 [-1.427, 14.124] | 0.745 [-1.835, 6.241] |
| 2018 | 24 | 18 | 0 | 6 | 3.426 [-3.758, 9.231] | 4.183 [2.884, 6.668] |
| 2022 | 17 | 10 | 0 | 7 | 0.773 [-5.154, 23.321] | 9.607 [2.492, 10.387] |


All displayed 12-decimal member sums pass at 1e-10; maximum residual 3.00e-12 seats. No residual is assigned to a party to force displayed closure.

## Files and regeneration

`party_components_all_years.csv` and its alias `party_AB_by_year.csv` contain 99 party-elections. `inversion_AB_summary.csv` contains 92 configurations; `inversion_party_components.csv` and its alias `inversion_party_AB.csv` contain 1327 complete case-party rows, including all deletion diagnostics. The two report Markdown files are identical. `party_AB_scatter.pdf`/PNG use the unchanged party-level numerical panel.

Rebuild with `python3 processing/Processing/decomposition/party_AB_diagnostic.py` after the normal Julia decomposition, or use `processing/rebuild_manuscript.sh --freeze-prose` for the complete integrated workflow. The default consumes the already-validated complete accounting panel and pinned cabinet release; it does not download electoral or cabinet inputs.
