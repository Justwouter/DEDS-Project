#### Archieve of all databases within CBS
https://opendata.cbs.nl/ODataCatalog/Tables?$format=json
#### Github repo of an python CBS library. Not necessarily useful but fun to look at
https://github.com/J535D165/cbsodata/blob/master/cbsodata/cbsodata3.py

#### CBS Gemiddelde verkoop prijzen in DH
https://opendata.cbs.nl/ODataApi/odata/83625NED/UntypedDataSet?$filter=((Perioden+eq+%271995JJ00%27)+or+(Perioden+eq+%272000JJ00%27)+or+(Perioden+eq+%272005JJ00%27)+or+(Perioden+eq+%272010JJ00%27)+or+(Perioden+eq+%272015JJ00%27)+or+(Perioden+eq+%272020JJ00%27)+or+(Perioden+eq+%272021JJ00%27)+or+(Perioden+eq+%272022JJ00%27))+and+((RegioS+eq+%27GM0518%27))&$select=Perioden,+RegioS,+GemiddeldeVerkoopprijs_1

https://opendata.cbs.nl/statline/#/CBS/nl/dataset/83625NED/table?ts=1684154786149

#### CBS Woningvooraad in DH
https://opendata.cbs.nl/ODataApi/odata/83704NED/UntypedDataSet?$filter=((Woningtype eq 'T001100') or (Woningtype eq 'ZW10290') or (Woningtype eq 'ZW10340')) and ((Oppervlakteklasse eq 'T001116') or (Oppervlakteklasse eq 'A041692') or (Oppervlakteklasse eq 'A025407') or (Oppervlakteklasse eq 'A025408') or (Oppervlakteklasse eq 'A025409') or (Oppervlakteklasse eq 'A025410') or (Oppervlakteklasse eq 'A025411') or (Oppervlakteklasse eq 'A025412') or (Oppervlakteklasse eq 'A041691')) and ((RegioS eq 'GM0518'))&$select=Woningtype, Oppervlakteklasse, Perioden, RegioS, BeginstandWoningvoorraad_1

https://opendata.cbs.nl/#/CBS/nl/dataset/83704NED/table?searchKeywords=voorraad%20woningen