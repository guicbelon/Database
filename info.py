AVAILABLE_TIME_FRAMES =  {"1m", "2m","5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"}

SGS_INFO = {
    "SELIC": 11,
    "INPC": 188,
    "IPCA": 433,
    "IPCA-15": 7478,
    "IPCA-E": 10764,
    "IGP-10": 7447,
    "IGP-DI": 190,
    "IGP-M1DEC": 7448,
    "IGP-M2DEC": 7449,
    "IGP-M": 189,
    "IPC-FIPE1AQ": 7463,
    "IPC-FIPE2AQ": 272,
    "IPC-FIPE3AQ": 7464,
    "IPC-FIPEMENSAL": 193,
    "IPC-DI": 191,
    "IPC-C1": 17680,
    "IPC-3I": 17679,
    "IPCA-12M": 13522,
    "IPCALIVRES": 11428,
    "IPCA-COMERCIALIZAVEIS": 4447,
    "IPCA-NAOCOMERCIALIZAVEIS": 4448,
    "IPCA-MONITORADOS": 4449,
    "IPCA-DURAVEIS": 10843,
    "IPCA-SEMIDURAVEIS": 10842,
    "IPCA-NAODURAVEIS": 10841,
    "IPCA-SERVICOS": 10844,
    "IPCA-EX1": 1621,
    "IPCA-MS": 4466,
    "IPCA-DP": 16122,
    "IPCA-ARACAJU": 7479,
    "IPCA-BELEM": 7480,
    "IPCA-BELOHORIZONTE": 7481,
    "IPCA-BRASILIA": 7482,
    "IPCA-CURITIBA": 7483,
    "IPCA-FLORIANOPOLIS": 7484,
    "IPCA-FORTALEZA": 7485,
    "IPCA-GOIANIA": 7486,
    "IPCA-JOAOPessoa": 7487,
    "IPCA-NATAL": 7488,
    "IPCA-PORTOALEGRE": 7489,
    "IPCA-RECIFE": 7490,
    "IPCA-RIOJANEIRO": 7491,
    "IPCA-SALVADOR": 7492,
    "IPCA-SAOPAULO": 7493,
    "IPCA-VITORIA": 7494,
    "PRODUCAOINDUSTRIALTOTAL": 21859,
    "PRODUCAOINDUSTRIAINDUSTRIADETRANSFORMACAO": 21862,
    "PRODUCAOINDUSTRIAEXTRATIVAMINERAL": 21861,
    "PRODUCAOACEOBRUTO": 7357,
    "PRODUCAODEINSUMOSDACONSTRUCAOCIVIL": 21868,
    "CONSULTASAOSPC": 1453,
    "CONSULTAAOUSECHEQUE": 1454,
    "VENDASREAISABRAS": 7414,
    "TRAFEGODEVEICULOSPESADASENASPEDAGIADAS": 28552,
    "CONSULTAAOSERASA": 28547,
    "UCIFGV": 24352,
    "UCICNI": 24351,
    "VENDASINDUSTRIAISREAIS": 1338,
    "HORASTRABALHADASNAPRODUCAODAINDUSTRIADETRANSFORMACAO": 24348,
    "SALARIOREALNAINDUSTRIADETRANSFORMACAO": 28558,
    "MASSASALARIALREALNAINDUSTRIADETRANSFORMACAO": 24349,
    "PRODUCAO-DESSAZ": 28527,
    "LICENCIAMENTOSDEAUTOVEICULOSNACIONAISNOVOS-DESSAZ": 28529,
    "EXPORTACAODEAUTOVEICULOS-DESSAZ": 28530,
    "TOTALLICENCIAMENTOSEEXPORTACOES-DESSAZ": 28528,
    "PRODUCAO": 1373,
    "LICENCIAMENTOSDEAUTOVEICULOSNACIONAISNOVOS": 1379,
    "EXPORTACAODEAUTOVEICULOS": 1380,
    "TOTALLICENCIAMENTOSEEXPORTACOES": 1378,
    "PRODUCAOINDUSTRIALTOTAL-DESSAZ": 28503,
    "PRODUCAOINDUSTRIAINDUSTRIADETRANSFORMACAO-DESSAZ": 28505,
    "PRODUCAOINDUSTRIAEXTRATIVAMINERAL-DESSAZ": 28504,
    "PRODUCAOACEOBRUTO-DESSAZ": 28546,
    "PRODUCAODEINSUMOSDACONSTRUCAOCIVIL-DESSAZ": 28511,
    "CONSULTASAOSPCEUSECHEQUE-DESSAZ": 28550,
    "CONSULTAAOSERASA-DESSAZ": 28548,
    "VENDASREAISABRAS-DESSAZ": 28549,
    "UCIFGV-DESSAZ": 28561,
    "UCICNI-DESSAZ": 28554,
    "VENDASINDUSTRIAISREAIS-DESSAZ": 28555,
    "HORASTRABALHADASNAPRODUCAODAINDUSTRIADETRANSFORMACAO-DESSAZ": 28556,
    "SALARIOREALNAINDUSTRIADETRANSFORMACAO-DESSAZ": 28559,
    "MASSASALARIALREALNAINDUSTRIADETRANSFORMACAO-DESSAZ": 28560,
    "PRODUCAOINDUSTRIALGERAL": 21858,
    "PRODUCAODEBENSDECAPITAL": 21863,
    "PRODUCAODEBENSINTERMEDIARIOS": 21864,
    "PRODUCAODEBENSDECONSUMOGERAL": 21865,
    "PRODUCAODEBENSDECONSUMODURAVEIS": 21866,
    "PRODUCAODEBENSDECONSUMONAODURAVEISESEMIDURAVEIS": 21867,
    "BENSDECAPITAL-DESSAZ": 28506,
    "BENSINTERMEDIARIOS-DESSAZ": 28507,
    "BENSDECONSUMOGERAL-DESSAZ": 28508,
    "BENSDECONSUMODURAVEIS-DESSAZ": 28509,
    "BENSDECONSUMONAODURAVEISESEMIDURAVEIS-DESSAZ": 28510,
    "EXPORTACAODEBENSDECAPITAL": 28567,
    "IMPORTACAODEBENSDECAPITAL": 28568,
    "EXPORTACAODEBENSDECAPITAL-DESSAZ": 28569,
    "IMPORTACAODEBENSDECAPITAL-DESSAZ": 28570,
    "NIVELDEEMPREGOFORMALTOTAL": 25239,
    "NIVELDEEMPREGONAINDUSTRIADATRANSFORMACAO": 25241,
    "NIVELDEEMPREGONOCOMERCIO": 25256,
    "NIVELDEEMPREGONOSSERVICOS": 25257,
    "NIVELDEEMPREGONACONSTRUCAOCIVIL": 25255,
    "NIVELDEEMPREGOFORMALTOTAL-DESSAZ": 28512,
    "NIVELDEEMPREGONAINDUSTRIADATRANSFORMACAO-DESSAZ": 28513,
    "NIVELDEEMPREGONOCOMERCIO-DESSAZ": 28514,
    "NIVELDEEMPREGONOSSERVICOS-DESSAZ": 28515,
    "NIVELDEEMPREGONACONSTRUCAOCIVIL-DESSAZ": 28516,
    "OCUPADAS": 24379,
    "DESOCUPADAS": 24380,
    "FORCADETRABALHO": 24378,
    "PESSOASENIDADEPARATRABALHAR": 24370,
    "REMUNERACAOMEDIA-DEFLAC": 24381,
    "REMUNERACAOMEDIA-NOMINAL": 24382,
    "PESSOALOCUPADOCOMRENDIMENTO": 28543,
    "MASSASALARIAL": 28544,
    "PIBPRECOSCORRENTES": 1207,
    "PIBEMRDOULTIMOANO": 1208,
    "PIBEMUS": 7324,
    "POPULACAO": 21774,
    "PIBPERCAPITAPRECOSCORRENTES": 21775,
    "PIBPERCAPITAEMRDOULTIMOANO": 21777,
    "PIBPERCAPITAEMUS": 21776,
    "PIBTRIMESTRAL": 22099,
    "CONSUMODASFAMILIASTRIMESTRAL": 22100,
    "CONSUMODOGOVERNOTRIMESTRAL": 22101,
    "FORMACAOBRUTADECAPITALFIXOFBCFTRIMESTRAL": 22102,
    "EXPORTACAOTRIMESTRAL": 22103,
    "IMPORTACAOTRIMESTRAL": 22104,
    "PIBDESSAZTRIMESTRAL": 22109,
    "CONSUMODASFAMILIASDESSAZTRIMESTRAL": 22110,
    "CONSUMODOGOVERNODESSAZTRIMESTRAL": 22111,
    "FBCFDESSAZTRIMESTRAL": 22113,
    "EXPORTACAODESSAZTRIMESTRAL": 22114,
    "IMPORTACAODESSAZTRIMESTRAL": 22115,
    "RESERVASINTERNACIONAISTOTAL": 13621,
    "TAXAREFERENCIAL": 226,
}
