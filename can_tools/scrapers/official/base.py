class TableauMapClick(StateDashboard, ABC):
    """
    Defines a few commonly-used helper methods for snagging Tableau data
    from mapclick-driven dashboard pages specifically
    """

    def getTbluMapFilter(self, htmDump) -> List:
        """
        Extracts the onMapClick background data filter function from a raw tableau HTML bootstrap return

        Parameters
        ----------
        htmdump : json
            The raw json-ized output of the info field from getRawTbluPageData

        Returns
        -------
        _ : List
            The Tableau-view-specific json filter function called onMapClick
        """
        urlFltr = []
        # Grab the map filter function guts:
        for fn in htmDump["worldUpdate"]["applicationPresModel"]["workbookPresModel"][
            "dashboardPresModel"
        ]["userActions"]:
            if fn.get("name") == "Map filter":
                urlFltr = (
                    urllib.parse.unquote(fn.get("linkSpec").get("url"))
                    .split("?")[1]
                    .replace("=<Countynm1~na>", "")
                    .split("&")
                )
        return urlFltr

    def extractTbluData(self, htmdump, area) -> pd.DataFrame:
        """
        Extracts data from raw tableau HTML bootstrap return

        Parameters
        ----------
        htmdump : json
            The raw json-ized output of the fdat data field from getRawTbluPageData

        area : the FIPS code of the htmdump

        Returns
        -------
        _ : pd.DataFrame
            Already-pivoted covid data with column names extracted from tableau
            'location' column will contain the proveded 'area' data
        """
        valF = []  # Initialize placeholder array
        # Grab the raw data loaded into the current tableau view
        lsUpdt = htmdump["secondaryInfo"]["presModelMap"]["dataDictionary"][
            "presModelHolder"
        ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][2][
            "dataValues"
        ][
            -1
        ]
        intDat = htmdump["secondaryInfo"]["presModelMap"]["dataDictionary"][
            "presModelHolder"
        ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][0][
            "dataValues"
        ]
        rlDat = htmdump["secondaryInfo"]["presModelMap"]["dataDictionary"][
            "presModelHolder"
        ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][1][
            "dataValues"
        ]
        # First extract the datatype and indices:
        for i in htmdump["secondaryInfo"]["presModelMap"]["vizData"]["presModelHolder"][
            "genPresModelMapPresModel"
        ]["presModelMap"]:
            dtyp = htmdump["secondaryInfo"]["presModelMap"]["vizData"][
                "presModelHolder"
            ]["genPresModelMapPresModel"]["presModelMap"][i]["presModelHolder"][
                "genVizDataPresModel"
            ][
                "paneColumnsData"
            ][
                "vizDataColumns"
            ][
                1
            ].get(
                "dataType"
            )
            indx = htmdump["secondaryInfo"]["presModelMap"]["vizData"][
                "presModelHolder"
            ]["genPresModelMapPresModel"]["presModelMap"][i]["presModelHolder"][
                "genVizDataPresModel"
            ][
                "paneColumnsData"
            ][
                "paneColumnsList"
            ][
                0
            ][
                "vizPaneColumns"
            ][
                1
            ].get(
                "aliasIndices"
            )[
                0
            ]
            if dtyp == "integer":
                valF.append([area, i, intDat[indx]])
            elif dtyp == "real":
                valF.append([area, i, rlDat[indx]])
        valF.append([area, "Last update", lsUpdt])
        if valF:
            val = pd.DataFrame(valF, columns=["location", "Name", "Value"])
            val = pd.pivot_table(
                val,
                values="Value",
                index=["location"],
                columns="Name",
                aggfunc="first",
            ).reset_index()
            return val
        else:
            return None

    def getRawTbluPageData(self, url, bsRt, reqParams) -> (json, json):
        """
        Extracts and parses htm data from a tableau dashboard page

        Parameters
        ----------
        url : str
            The root of the Tableau dashboard

        bsRt : str
            The bootstrap root url.
            Typically everything before the first '/' delimiter in 'url'

        reqParams : dict
            Dictionary of request parameters useable by 'requests' library

        Returns
        -------
        info, fdat : (json, json)
            'info' is the header section of the Tableau dashboard page (as json)
            'fdat' is the data section of the Tableau dashboard page (as json)
        """
        # Initialize main page: grab session ID key, sheet ID key, root directory string
        r = requests.get(url, params=reqParams)

        # Parse the output, return a json so we can build a bootstrap call
        suppe = BeautifulSoup(r.text, "html.parser")
        tdata = json.loads(suppe.find("textarea", {"id": "tsConfigContainer"}).text)

        # Call the bootstrapper: grab the state data, map selection update function
        dataUrl = f'{bsRt}{tdata["vizql_root"]}/bootstrapSession/sessions/{tdata["sessionid"]}'
        r = requests.post(
            dataUrl,
            data={"sheet_id": tdata["sheetId"], "showParams": tdata["showParams"]},
        )

        # Regex the non-json output
        dat = re.search("\d+;({.*})\d+;({.*})", r.text, re.MULTILINE)

        # load info head and data group separately
        info = json.loads(dat.group(1))
        fdat = json.loads(dat.group(2))

        return (info, fdat)
