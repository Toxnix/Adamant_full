import React, { useCallback, useState } from "react";
import $ from "jquery";
//import { makeStyles } from "@mui/styles";
import { useDropzone } from "react-dropzone";
import { useHistory } from 'react-router-dom';
import { useDispatch} from 'react-redux';
import { logoutSuccess } from '../redux/actions/authActions';
//import QPTDATLogo from "../assets/header-image.png";
import FormRenderer from "../components/FormRenderer";
import Button from "@mui/material/Button";
import { TextField } from "@mui/material";
import Divider from "@mui/material/Divider";
import { FormContext } from "../FormContext";
import array2object from "../components/utils/array2object";
import object2array from "../components/utils/object2array";
import DownloadIcon from "@mui/icons-material/GetApp";
import set from "set-value";
import getValue from "../components/utils/getValue";
import CryptoJS from "crypto-js";
import deleteKeySchema from "../components/utils/deleteKeySchema";
import validateAgainstSchema from "../components/utils/validateAgainstSchema";
import { useEffect } from "react";
import { toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import array2objectAnyOf from "../components/utils/array2objectAnyOf";
import SchemaOne from "../schemas/all-types.json";
import SchemaTwo from "../schemas/demo-schema.json";
import SchemaThree from "../schemas/example-experiment-schema.json";
import SchemaFour from "../schemas/example-request-schema.json";
import SchemaFive from "../schemas/plasma-mds.json";
import convData2FormData from "../components/utils/convData2FormData";
import changeKeywords from "../components/utils/changeKeywords";
//import QPTDATLogo from "../assets/adamant-header-5.svg";
import QPTDATLogo from "../assets/adamant-header-5.svg";
import EMPIRFLogo from "../assets/EMPI_Logo_reactive-fluids_Color_Black.png"
import validateSchemaAgainstSpecification from "../components/utils/validateSchemaAgainstSpecification";
import { Autocomplete } from "@mui/material";
import checkIDexistence from "../components/utils/checkIDexistence";
// import ReactMuiDemoWrapper from '../components/ReactMuiDemoWrapper';

// function that receive the schema and convert it to Form/json data blueprint
// also to already put the default value to this blueprint
const createFormDataBlueprint = (schemaProperties) => {
  let newObject = {};

  Object.keys(schemaProperties).forEach((item) => {
    if (schemaProperties[item]["type"] !== "object") {
      if (schemaProperties[item]["default"] !== undefined) {
        newObject[item] = schemaProperties[item]["default"];
      } else if (
        (schemaProperties[item]["default"] === undefined) &
        (schemaProperties[item]["enum"] !== undefined)
      ) {
        newObject[item] = schemaProperties[item]["enum"][0];
      } else if (
        (schemaProperties[item]["type"] === "boolean") &
        (schemaProperties[item]["default"] === undefined)
      ) {
        newObject[item] = false;
      }
    } else {
      if (schemaProperties[item]["properties"] !== undefined) {
        newObject[item] = createFormDataBlueprint(
          schemaProperties[item]["properties"]
        );
      }
    }
  });

  return newObject;
};

// function to remove empty artributes
const removeEmpty = (obj) => {
  Object.keys(obj).forEach((key) => {
    if (obj[key] && typeof obj[key] === "object") {
      const childObject = removeEmpty(obj[key]);
      if (childObject === undefined) {
        delete obj[key];
      }
    } else if (obj[key] === "" || obj[key] === null || obj[key] === undefined) {
      delete obj[key];
    }
  });
  return Object.keys(obj).length > 0 || obj instanceof Array ? obj : undefined;
};

const AdamantMain = ({ onLogout }) => {
  // state management
  const [disable, setDisable] = useState(true);
  const [schemaMessage, setSchemaMessage] = useState(null);
  const [schemaValidity, setSchemaValidity] = useState(false);
  const [schema, setSchema] = useState(null);
  const [schemaIntermediate, setSchemaIntermediate] = useState(null);
  const [renderReady, setRenderReady] = useState(false);
  const [editMode, setEditMode] = useState(true);
  const [schemaList, setSchemaList] = useState([]);
  const [schemaNameList, setSchemaNameList] = useState([]);
  const [selectedSchemaName, setSelectedSchemaName] = useState("");
  const [originalSchema, setOriginalSchema] = useState();
  const [inputMode, setInputMode] = useState(false);
  const [convertedSchema, setConvertedSchema] = useState(null);
  const [createScratchMode, setCreateScratchMode] = useState(false);
  const [browseSchemaMode, setBrowseSchemaMode] = useState(false);
  const [jsonData, setJsonData] = useState({});
  const [descriptionList, setDescriptionList] = useState("");
  const [schemaWithValues, setSchemaWithValues] = useState({});
  const [schemaSpecification, setSchemaSpecification] = useState("");
  const [onlineMode, setOnlineMode] = useState(false);
  const [SEMSelectedDevice, setSEMSelectedDevice] = useState("");
  const [HeaderImage, setHeaderImage] = useState(QPTDATLogo);
  const [EMPIRFHeaderImage, setEMPIRFHeaderImage] = useState(EMPIRFLogo);
  const [jobRequestSchemas, setJobRequestSchemas] = useState([]);
  const [submitTextList, setSubmitTextList] = useState([]);
  const [setSubmitText] = useState("Submit Job Request");
  const [browseExpirementsMode, setBrowseExpirementsMode] = useState(false);
  const dispatch = useDispatch();
  const dbUiUrl =
    window.location.port === "3000"
      ? "http://localhost:3001/db-ui/"
      : "/db-ui/";
  useEffect(() => {
    document.body.style.overflow = browseExpirementsMode ? "hidden" : "";
    return () => {
      document.body.style.overflow = "";
    };
  }, [browseExpirementsMode]);
  // for dropdown buttons
  const [setAnchorEl] = useState(null);

  const handleClose = () => {
    setAnchorEl(null);
  }; //

  // loaded files object
  const [loadedFiles, setLoadedFiles] = useState([]);

  let implementedFieldTypes = [
    "string",
    "number",
    "integer",
    "array",
    "boolean",
    "object",
  ];

  // check if the front-end is connected to backend at all
  useEffect(() => {
    $.ajax({
      type: "GET",
      url: "/api/check_mode",
      success: function (status) {
        console.log("Connection to server is established. Online mode");
        setJobRequestSchemas(status["jobRequestSchemaList"]);
        console.log(status["jobRequestSchemaList"]);
        setSubmitTextList(status["submitButtonText"]);
        setOnlineMode(true);
        toast.success(
          <>
            <div>
              <strong>Connection to server is established.</strong>
            </div>
          </>,
          {
            toastId: "connectionSuccess",
          }
        );
      },
      error: function () {
        console.log(
          "Unable to establish connection to server. Offline mode. Submit feature is disabled."
        );
        setOnlineMode(false);

        // use available schema as a place holder
        setSchemaNameList([
          "",
          "all-types.json",
          "demo-schema.json",
          "example-experiment-schema.json",
          "example-request-schema.json",
          "plasma-mds.json",
        ]);
        setSchemaList([
          null,
          SchemaOne,
          SchemaTwo,
          SchemaThree,
          SchemaFour,
          SchemaFive,
        ]);

        toast.warning(
          <>
            <div>
              <strong>Unable to establish connection to server.</strong>
            </div>
            <div>Submit feature is disabled.</div>
          </>,
          {
            toastId: "connectionWarning",
          }
        );
      },
    });
  }, []);

  // get schemas from server when onlinemode is true
  useEffect(() => {
    // if online mode then get available schemas from server
    if (onlineMode === true) {
      $.ajax({
        type: "GET",
        url: "/api/get_schemas",
        success: function (status) {
          console.log("SUCCESS");

          // do this to preserver the order
          // let sch = [];
          // status["schema"].forEach((element) => {
	  //  sch.push(JSON.parse(element));
          // });
          // Initialize arrays to hold filtered results
            let sch = [];
            let schNames = [];

            // Loop through both arrays and filter out any pairs with null or empty schema content
            status["schema"].forEach((schemaContent, index) => {
                let schemaName = status["schemaName"][index];
                if (schemaContent && schemaContent.trim() && schemaName && schemaName.trim()) {
                    // Parse and push to the filtered arrays if both schema and schemaName are valid
                    sch.push(JSON.parse(schemaContent));
                    schNames.push(schemaName);
                }
            });

            // Set state with filtered schema and schemaName lists, maintaining the order
            setSchemaList(sch);
            setSchemaNameList(schNames);
        },
        error: function () {
          console.log("ERROR");
          toast.warning(
            "Error while fetching the schemas. Using basic list of schemas.",
            {
              toastId: "fetchingSchemasError",
            }
          );
          // if unable to fetch the schemas then use the basic list of schemas
          setSchemaNameList([
            "",
            "all-types.json",
            "demo-schema.json",
            "example-experiment-schema.json",
            "example-request-schema.json",
            "plasma-mds.json",
          ]);
          setSchemaList([
            null,
            SchemaOne,
            SchemaTwo,
            SchemaThree,
            SchemaFour,
            SchemaFive,
          ]);
        },
      });
    }
  }, [onlineMode]);

  // Handle save schema button click
  const handleOnClickSaveSchema = () => {
    let content = { ...schema }; // Assuming schemaContent is an object
    // Log the content to see its structure
    console.log('Schema content:', content);
  
    // Extract the SchemaID from the schema properties
    const schemaID = content?.$id;

    // Log the extracted SchemaID
    console.log('Extracted SchemaID:', schemaID);

    // Check if SchemaID is present
    if (!schemaID) {
      console.error('SchemaID is missing or empty.');
      toast.error('Schema ID cannot be empty. Please provide a valid Schema ID.', {
        toastId: 'schemaIDError',
      });
      return; // Exit the function if SchemaID is empty
    }

    // Proceed with saving the schema if SchemaID is valid
    const schemaData = {
      schemaName: schemaID, // Use the SchemaID as the schema name
      schema: JSON.stringify(content, null, 2), // Save the schema content
    };
  
    if (schemaData) { // If a name is provided by the user
      fetch('/api/save_schema', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(schemaData),
      })
      .then(response => {
        if (response.ok) {
          console.log('Schema saved successfully');
          toast.success('Schema saved successfully!', {
            toastId: 'savingSchemaSuccess',
          });
        } else {
          throw new Error('Failed to save schema');
        }
      })
      .catch(error => {
        console.error('Failed to save schema:', error);
        toast.error('Error saving schema. Please try again.', {
          toastId: 'savingSchemaError',
        });
      });
    } else {
      console.error('Error saving schema. Please Enter a Schema Name.');
      toast.error('Error saving schema. Please Enter a Schema Name.', {
        toastId: 'savingSchemaError',
      });
    }
  };
  
  const history = useHistory();
  const handleLogout = () => {
    // Remove session token from localStorage
    localStorage.removeItem('sessionToken');
    dispatch(logoutSuccess());
    // Optionally, redirect to login page
    history.push('/');
  };

  // handle select schema on change
  const handleSelectSchemaOnChange = (schemaName) => {
    if (schemaName === null) {
      clearSchemaOnClick()

      return 
    }
    
    //console.log(event)
    // first reset states
    setRenderReady(false);
    setDisable(true);
    setCreateScratchMode(false);
    setJsonData({});
    //

    console.log("selected schema:", schemaName);
    setSelectedSchemaName(schemaName);

    let selectedSchema = schemaList[schemaNameList.indexOf(schemaName)];

    // reset everything when selectedSchema is empty
    if (selectedSchema === null) {
      setDisable(true);
      setRenderReady(false);
      setSchema(null);
      setSchemaValidity(false);
      setSchemaMessage();
      setCreateScratchMode(false);
      setJsonData({});
      return;
    }

    // convert selectedSchema schema to iterable array properties
    let convertedSchema = JSON.parse(JSON.stringify(selectedSchema));
    try {
      convertedSchema["properties"] = object2array(
        selectedSchema["properties"]
      );

      // update states
      setSchemaValidity(true);
      setSchemaMessage(`${schemaName} is a valid schema`);
      setSchema(selectedSchema);
      let oriSchema = JSON.parse(JSON.stringify(selectedSchema));
      setOriginalSchema(oriSchema);
      setSchemaWithValues(JSON.parse(JSON.stringify(oriSchema)));
      setConvertedSchema(convertedSchema);

      if (jobRequestSchemas.includes(convertedSchema["title"])) {
        try {
          //let SEMlogo = require("../assets/sem-header-picture.png");
          //setHeaderImage(SEMlogo["default"]);
          setHeaderImage(QPTDATLogo);
          setEMPIRFHeaderImage(EMPIRFHeaderImage);
          setEditMode(false);
          setSubmitText(
            submitTextList[jobRequestSchemas.indexOf(convertedSchema["title"])]
          );
        } catch (error) {
          console.log(error);
          setHeaderImage(QPTDATLogo);
          setEMPIRFHeaderImage(EMPIRFHeaderImage);
          setEditMode(true);
        }
      } else {
        setHeaderImage(QPTDATLogo);
        setEMPIRFHeaderImage(EMPIRFHeaderImage);
        setEditMode(true);
      }

      // create form data
      let formData = createFormDataBlueprint(selectedSchema["properties"]);
      setJsonData(formData);
    } catch (error) {
      console.log(error);
      // update states
      setSchemaValidity(false);
      setSchemaMessage(`${schemaName} is invalid`);
      setSchema(null);
    }
  };

  // function to check if the file accepted is of json format and json schema valid
  const checkSchemaValidity = (schemaFile) => {
    // place holder
    if (schemaFile[0]["type"] === "application/json") {
      // read the file with FileReadr API
      const reader = new FileReader();
      reader.onabort = () => console.log("file reading was aborted");
      reader.onerror = () => console.log("file reading has failed");
      reader.onload = () => {
        const binaryStr = reader.result;
        const obj = JSON.parse(binaryStr);

        // convert obj schema to iterable array properties
        let convertedSchema = JSON.parse(JSON.stringify(obj));
        try {
          convertedSchema["properties"] = object2array(obj["properties"]);

          // update states
          setSchemaValidity(true);
          setSchemaMessage(`${schemaFile[0]["name"]} is a valid schema`);
          setSchema(obj);
          let oriSchema = JSON.parse(JSON.stringify(obj));
          setOriginalSchema(oriSchema);
          setSchemaWithValues(JSON.parse(JSON.stringify(oriSchema)));
          setConvertedSchema(convertedSchema);

          if (jobRequestSchemas.includes(obj["title"])) {
            try {
              //let SEMlogo = require("../assets/sem-header-picture.png");
              //setHeaderImage(SEMlogo["default"]);
              setHeaderImage(QPTDATLogo);
              setEMPIRFHeaderImage(EMPIRFHeaderImage);
              setEditMode(false);
              setSubmitText(
                submitTextList[
                  jobRequestSchemas.findIndex(convertedSchema["title"])
                ]
              );
            } catch (error) {
              console.log(error);
              setHeaderImage(QPTDATLogo);
              setEMPIRFHeaderImage(EMPIRFHeaderImage);
              setEditMode(true);
            }
          } else {
            setHeaderImage(QPTDATLogo);
            setEMPIRFHeaderImage(EMPIRFHeaderImage);
            setEditMode(true);
          }

          // create form data
          let formData = createFormDataBlueprint(obj["properties"]);
          setJsonData(formData);
        } catch (error) {
          console.log(error);
          // update states
          setSchemaValidity(false);
          setSchemaMessage(`${schemaFile[0]["name"]} is invalid`);
          setSchema(null);
        }
      };
      reader.readAsText(schemaFile[0]);
    } else {
      // update states
      setSchemaValidity(false);
      setSchemaMessage(`${schemaFile[0]["name"]} is of incorrect file type`);
      setSchema(null);
    }
  };

  // browse or drag&drop schema file
  const onDrop = useCallback(
    (acceptedFile) => {
      // process the schema, validation etc
      setBrowseSchemaMode(true);
      console.log("In onDrop compilation: browseSchemaMode =", browseSchemaMode);
      checkSchemaValidity(acceptedFile);
      // store schema file in the state
      // update states
      setRenderReady(false);
      setDisable(true);
      setCreateScratchMode(false);
      console.log("In onDrop compilation: createSchratchMode =", createScratchMode);
      setJsonData({});
      setSelectedSchemaName("");
    },
    [browseSchemaMode, checkSchemaValidity, createScratchMode, setRenderReady]
  );
  //

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    multiple: false,
  });

  // render on-click handle
  const renderOnClick = () => {
    //setFormRenderInProgress(true);
    setDisable(false);
    setRenderReady(true);
  };

  // clear schema on-click handle
  const clearSchemaOnClick = () => {
    setHeaderImage(QPTDATLogo);
    setEMPIRFHeaderImage(EMPIRFHeaderImage);
    setDisable(true);
    setRenderReady(false);
    setSchema(null);
    setSchemaValidity(false);
    setSchemaMessage();
    setCreateScratchMode(false);
    setSelectedSchemaName("");
  };

  // create new schema from scratch
  const createSchemaFromScratch = () => {
    // update browse schema render states
    setSchemaValidity(false);
    setSchemaMessage();
    setJsonData({});
    setSelectedSchemaName("");
    setBrowseExpirementsMode(false);

    const now = new Date();
    
    // ISO 8601 Date: 'YYYY-MM-DD'
    const isoDate = now.toISOString().split('T')[0];
    
    // ISO 8601-1:2019 Time: 'HH:mm:ss'
    const isoTime = now.toISOString().split('T')[1].split('.')[0];  // Extracts time without milliseconds


    // always use newer schema specification
    let schemaBlueprint = {
      $schema: "http://json-schema.org/draft-07/schema#",
      type: "object",
      properties: {
        "FileTypeIdentifier": {
          "title": "FileTypeIdentifier",
          "description": "to allow crawlers to idendify our JSON files",
          "type": "string",
          "default": "This is a EMPI-RF metadata File. Do not change this for crawler identification",
          "enum": [
            "This is a EMPI-RF metadata File. Do not change this for crawler identification"
          ]
        },
        "Identifier": {
          "title": "Identifier",
          "description": "Unique identifier for the dataset",
          "type": "string"
        },
        "Creator": {
          "title": "Creator",
          "description": "The name of the primary researcher",
          "type": "string"
        },
        "ORCID": {
          "title": "ORCID",
          "description": "ID of the primary researcher",
          "type": "string"
        },
        "Date": {
          "title": "Date",
          "description": "The date when the experiment started in ISO 8601",
          "type": "string",
          "default": isoDate
        },
        "Time": {
          "title": "Time",
          "description": "The time when the experiment started in ISO 8601-1:2019",
          "type": "string",
          "default": isoTime
        },
        "Project": {
          "title": "Project",
          "description": "Name of the related project",
          "type": "string"
        },
      },
      required: [
        "FileTypeIdentifier",
        "Identifier",
        "Creator",
        "ORCID",
        "Date",
        "Time",
        "Project"
      ]
    };
    const obj = JSON.parse(JSON.stringify(schemaBlueprint));

    // create form data again
    let formData = createFormDataBlueprint(obj["properties"]);
    setJsonData(formData);

    // convert obj schema to iterable array properties
    let convertedSchema = JSON.parse(JSON.stringify(obj));
    convertedSchema["properties"] = object2array(obj["properties"]);

    // update states
    setCreateScratchMode(true);
    console.log("In handlCreateFromScratch compilation: createScratchMode =", createScratchMode);
    setSchema(obj);
    let oriSchema = JSON.parse(JSON.stringify(obj));
    setOriginalSchema(oriSchema);
    setSchemaWithValues(JSON.parse(JSON.stringify(oriSchema)));
    setConvertedSchema(convertedSchema);

    if (jobRequestSchemas.includes(obj["title"])) {
      try {
        //let SEMlogo = require("../assets/sem-header-picture.png");
        //setHeaderImage(SEMlogo["default"]);
        setHeaderImage(QPTDATLogo);
        setEMPIRFHeaderImage(EMPIRFHeaderImage);
        setEditMode(false);
        setSubmitText(
          submitTextList[jobRequestSchemas.findIndex(convertedSchema["title"])]
        );
      } catch (error) {
        console.log(error);
        setHeaderImage(QPTDATLogo);
        setEMPIRFHeaderImage(EMPIRFHeaderImage);
        setEditMode(true);
      }
    } else {
      setHeaderImage(QPTDATLogo);
      setEMPIRFHeaderImage(EMPIRFHeaderImage);
      setEditMode(true);
    }

    setDisable(false);
    setRenderReady(true);
  };

  const handleBrowseExpirementsResults = () => {
    setBrowseExpirementsMode(true);
    setRenderReady(false);
    setEditMode(false);
    setCreateScratchMode(false);
  };

  const handleReturnHome = () => {
    setBrowseExpirementsMode(false);
    setRenderReady(false);
    setEditMode(false);
    setCreateScratchMode(false);

    // window.location.reload();
  };

  // compile on-click handle
  const compileOnClick = () => {
    let value = schema;

    console.log("Before compilation: browseSchemaMode =", browseSchemaMode);
    console.log("Before compilation: createSchratchMode =", createScratchMode);

    const [valid, message] = validateSchemaAgainstSpecification(
      JSON.parse(JSON.stringify(schema)),
      schemaSpecification
    );
    if (valid) {

      setInputMode(true);
      setSchema(value);
      setEditMode(false);
      setDisable(true);
    } else {
      toast.error(
        <>
          <div>
            <strong>Your schema is not valid.</strong>
          </div>
          {message}
        </>,
        {
          toastId: "schemaError",
        }
      );
      return;
    }
  };

  // return to edit mode handle
  const toEditMode = () => {
    let value = schema;
    if (jobRequestSchemas.includes(schema["title"])) {
      setInputMode(false);
      setSchema(value);
      setEditMode(false);
      setDisable(false);
    } else {
      setInputMode(false);
      setSchema(value);
      setEditMode(true);
      setDisable(false);
    }
  };

  // update parent (re-render everything)
  const updateParent = (value) => {
    let newValue = { ...value };

    if (newValue["$schema"] === "http://json-schema.org/draft-04/schema#") {
      changeKeywords(newValue, "$id", "id");
    } else {
      changeKeywords(newValue, "id", "$id");
    }

    // update original schema
    let updatedSchema = JSON.parse(JSON.stringify(newValue));
    let tempSchema = JSON.parse(JSON.stringify(newValue));
    updatedSchema["properties"] = array2object(tempSchema["properties"]);

    setConvertedSchema(newValue);
    setSchema(updatedSchema);

    // update intermediate schema
    let updatedSchema2 = JSON.parse(JSON.stringify(newValue));
    let tempSchema2 = JSON.parse(JSON.stringify(newValue));
    updatedSchema2["properties"] = array2objectAnyOf(tempSchema2["properties"]);
    setSchemaIntermediate(updatedSchema2);
  };

  // update error stuff visually after validation (if some field(s) is are invalid)
  const setErrorStuffUponValidation = (errorMessages) => {
    let value = { ...convertedSchema };
    errorMessages.forEach((message) => {
      let path = message.path;
      path = path.split(".");
      let newPath = [];
      let tempValue = JSON.parse(JSON.stringify(value));
      for (let i = 0; i < path.length; ) {
        if (path[i] === "items" && tempValue[path[i]]["type"] === "object") {
          set(value, newPath.join(".") + ".adamant_field_error", true);
          set(
            value,
            newPath.join(".") + ".adamant_error_description",
            "One or more fields in this array have invalid inputs. Please fix them."
          );
          return;
        }
        if (
          path[i] === "properties" &&
          Array.isArray(tempValue["properties"])
        ) {
          newPath.push(path[i]);
          i += 1;
          let index = tempValue["properties"].findIndex(
            (val) => val.fieldKey === path[i]
          );
          newPath.push(index);
          i += 1;
          tempValue = tempValue["properties"][index];
        } else {
          newPath.push(path[i]);
          tempValue = tempValue[path[i]];
          i += 1;
        }
      }
      //console.log(newPath.join("."));
      set(value, newPath.join(".") + ".adamant_field_error", true);
      set(
        value,
        newPath.join(".") + ".adamant_error_description",
        message.message
      );
    });

    updateParent(value);
  };

  // revert all changes to the schema
  const revertAllChanges = () => {
    let value = { ...originalSchema };
    // convert obj schema to iterable array properties
    let convertedSchema = JSON.parse(JSON.stringify(value));
    convertedSchema["properties"] = object2array(value["properties"]);
    console.log(convertedSchema);
    setConvertedSchema(convertedSchema);
    setSchema(value);
    setSchemaWithValues(value);
    setDescriptionList("");

    // create form data again
    let formData = createFormDataBlueprint(value["properties"]);
    setJsonData(formData);
  };

  /*/ handle data input on blur
  const handleDataInput = (event, path, type) => {
    let jData = { ...jsonData };
    let value;
    if (["string", "number", "integer", "boolean"].includes(type)) {
      if (["number", "integer", "boolean"].includes(type)) {
        value = event;
      } else {
        value = event.target.value;
      }
    } else if (type === "array") {
      value = event;
    }
    set(jData, path, value);
    //console.log("Current form data    (jData):", jData);
    setJsonData(jData);
  };
  /*/

  // handle data input on blur to convertedSchema
  const handleConvertedDataInput = (event, path, type) => {
    let convSchemaData = { ...convertedSchema };
    let value;
    if (["string", "number", "integer", "boolean"].includes(type)) {
      if (["number", "integer", "boolean"].includes(type)) {
        value = event;
      } else {
        value = event.target.value;
      }
    } else if (type === "array") {
      value = event;
    }
    set(convSchemaData, path, value);
    setConvertedSchema(convSchemaData);
    console.log(convSchemaData);

    let data = convData2FormData(
      JSON.parse(JSON.stringify(convSchemaData["properties"]))
    );

    setJsonData(data);

    // convert to form data
    console.log("Current form data (convData):", data);

    // unconverted
    //console.log("Current form data (unconverted convData):", convSchemaData);
  };

  // delete data in jsonData when the field in schema is deleted
  const handleDataDelete = (path) => {
    console.log("path", path)
    console.log(jsonData)
    let jData = { ...jsonData };
    let value = deleteKeySchema(jData, path);
    setJsonData(value);
    console.log("Current form data:", value);
  };

  // handle check if id already exists in the schema
  const handleCheckIDexistence = (id) =>{
    let result = false
    result = checkIDexistence(schema, id, result);
    return result
  }

  // update form data id if a fieldkey changes, simply delete key value pair of the oldfieldid from jsonData
  const updateFormDataId = (
    oldFieldId,
    newFieldId,
    pathFormData,
    defaultValue
  ) => {
    if (oldFieldId === newFieldId) {
      return;
    }
    if (defaultValue === undefined) {
      let jData = { ...jsonData };
      jData = deleteKeySchema(jData, pathFormData);
      setJsonData(jData);
      console.log("Current form data:", jData);
    } else {
      let newPathFormData = pathFormData.split(".");
      newPathFormData.pop();
      newPathFormData.push(newFieldId);

      let jData = { ...jsonData };
      let value = getValue(jData, pathFormData);
      set(jData, newPathFormData, value);
      jData = deleteKeySchema(jData, pathFormData);
      setJsonData(jData);
      console.log("Current form data:", jData);
    }
  };

  // handle download json schema
  const handleDownloadJsonSchema = () => {
    let content = { ...schema };

    // calculate hash for the content
    // calculate hash using CryptoJS
    let sha256_hash = CryptoJS.SHA256(JSON.stringify(content));

    let a = document.createElement("a");
    let file = new Blob([JSON.stringify(content, null, 2)], {
      type: "application/json",
    });
    a.href = URL.createObjectURL(file);
    a.download = `jsonschema-${sha256_hash}.json`;
    a.click();

    handleClose();
  };

  // handle download json schema
  const handleDownloadFormData = () => {
    //let content = { ...jsonData };
    let convSchemaData = { ...convertedSchema };
    let content = convData2FormData(
      JSON.parse(JSON.stringify(convSchemaData["properties"]))
    );
    let contentSchema = { ...schema };

    // get rid of empty values in content
    content = removeEmpty(content);
    if (content === undefined) {
      content = {};
    }
    console.log("content", content);

    //
    // validate jsonData against its schema before download
    //
    const [valid, messages] = validateAgainstSchema(content, contentSchema);
    setErrorStuffUponValidation(messages);
    if (!valid | (Object.keys(content).length === 0)) {
      toast.error(
        <>
          <div>
            <strong>Form data is not valid.</strong>
          </div>
          <div style={{ paddingBottom: "10px" }}>Check your inputs!</div>
          {messages.map((item, index) => {
            return <div key={index}>{index + 1 + ". " + item.message}</div>;
          })}
        </>,
        {
          autoClose: 10000,
          toastId: "formDataError",
        }
      );
      return;
    }

    // Adding SchemaID parameter containing the selected schema name
    content.SchemaID = selectedSchemaName.replace(/\.json$/, '');
    // calculate hash for the content
    // calculate hash using CryptoJS
    let sha256_hash = CryptoJS.SHA256(JSON.stringify(content));

    let a = document.createElement("a");
    let file = new Blob([JSON.stringify(content, null, 2)], {
      type: "application/json",
    });
    a.href = URL.createObjectURL(file);
    a.download = `formdata-${sha256_hash}.json`;
    a.click();

    handleClose();
  };

  // gather all loaded files in one object
  const handleLoadedFiles = (file) => {
    let files = loadedFiles;
    //console.log(files);

    // check if file already exists
    let isFileAlreadyExist = false;
    for (let i = 0; i < files.length; i++) {
      if (files[i]["name"] === file["name"]) {
        isFileAlreadyExist = true;
      }
    }

    if (isFileAlreadyExist) {
      console.log("File already exists. Skipping it.");
      toast.warning(
        <>
          <div>
            <strong>File already loaded: {`${file["name"]}`}.</strong>
          </div>
        </>,
        {
          toastId: "fileAlreadyLoaded" + file["name"],
        }
      );
      //console.log("loaded files:", files);
      return true;
    } else {
      console.log("File not exist yet. Pushing it.");
      files.push(file);
      //console.log("loaded files:", files);
      setLoadedFiles(files);
      console.log("File added. Current files:", loadedFiles);
      toast.success(
        <>
          <div>
            <strong>File successfully loaded:</strong>
            {` ${file["name"]}`}.
          </div>
        </>,
        {
          toastId: "fileLoadedSuccessfully" + file["name"],
        }
      );
      return false;
    }
  };

  // remove file from loadedFiles based on its index
  const handleRemoveFile = (fileIndex) => {
    let files = loadedFiles;
    if (fileIndex > -1) {
      files.splice(fileIndex, 1);
      setLoadedFiles(files);
      console.log("File removed. Current files:", loadedFiles);
    } else {
      console.log("No file needs to be removed. Current files:", loadedFiles);
    }
  };

  return (
    <FormContext.Provider
      value={{
        loadedFiles,
        handleRemoveFile,
        handleLoadedFiles,
        updateParent,
        convertedSchema,
        updateFormDataId,
        handleDataDelete,
        handleConvertedDataInput,
        SEMSelectedDevice,
        schemaSpecification,
        setSchemaSpecification,
        setSEMSelectedDevice,
        implementedFieldTypes,
        handleCheckIDexistence,
      }}
    >
      <div
        style={{
          display: browseExpirementsMode ? "flex" : "block",
          flexDirection: browseExpirementsMode ? "column" : undefined,
          height: browseExpirementsMode ? "100vh" : undefined,
          minHeight: browseExpirementsMode ? 0 : undefined,
          minWidth: browseExpirementsMode ? 0 : undefined,
        }}
      >
        <div
          style={{
            paddingBottom: "5px",
            flex: browseExpirementsMode ? "0 0 auto" : undefined,
          }}
        >
          <img
            style={{ height: "100px", borderRadius: "2px", marginRight: "40px"}}
            alt="header"
            src={HeaderImage !== undefined ? HeaderImage : QPTDATLogo}
          />
          <img
            style={{ height: "70px", borderRadius: "5px" }}
            alt="empi-rf"
            src={EMPIRFHeaderImage !== undefined ? EMPIRFHeaderImage : EMPIRFLogo}
          />   
          {!inputMode ? (
            <div
              style={{
                display: "flex",
                textAlign: "left",
                padding: "10px 10px 0px 10px",
              }}
            >
              {!browseExpirementsMode && (
                <>
                  <Autocomplete
                    disablePortal
                    value={selectedSchemaName}
                    onChange={(event, newValue) =>
                      handleSelectSchemaOnChange(newValue)
                    }
                    id="select-available-schema"
                    options={schemaNameList.slice().sort((a, b) => a.localeCompare(b))}
                    style={{ width: "2600px" }}
                    renderInput={(params) => (
                      <TextField
                        variant="outlined"
                        {...params}
                        label="Select existing schema"
                      />
                    )}
                  />
                  <div
                    style={{
                      paddingLeft: "10px",
                      display: "flex",
                      justifyContent: "center",
                      alignItems: "center",
                    }}
                  >
                    OR
                  </div>
                </>
              )}
              <Button
                style={{ width: "100%", marginLeft: "10px" }}
                variant="contained"
                color="primary"
                {...getRootProps()}
              >
                <input {...getInputProps()} />
                {isDragActive ? "Drop here" : "Browse Schema"}
              </Button>
              <div
                style={{
                  paddingLeft: "10px",
                  display: "flex",
                  justifyContent: "center",
                  alignItems: "center",
                }}
              >
                OR
              </div>
              <Button
                onClick={() => createSchemaFromScratch()}
                style={{
                  width: "100%",
                  marginLeft: "10px",
                  marginRight: "10px",
                }}
                variant="contained"
                color="primary"
              >
                CREATE FROM SCRATCH
              </Button>
              <div
                style={{
                  paddingLeft: "10px",
                  display: "flex",
                  justifyContent: "center",
                  alignItems: "center",
                }}
              >
                OR
              </div>
              <Button
                onClick={() => handleBrowseExpirementsResults()}
                style={{
                  width: "100%",
                  marginLeft: "10px",
                  marginRight: "10px",
                }}
                variant="contained"
                color="primary"
              >
                BROWSE EXPIREMENTS RESULTS
              </Button>
              {browseExpirementsMode && (
              <>
                <div
                  style={{
                    paddingLeft: "10px",
                    display: "flex",
                    justifyContent: "center",
                    alignItems: "center",
                  }}
                >
                  OR
                </div>
                <Button
                  variant="contained"
                  color="primary"
                  style={{
                    width: "100%",
                    marginLeft: "5px",
                    marginRight: "5px",
                  }}
                  onClick={handleReturnHome}
                >
                  RETURN HOME
                </Button>
              </>
              )}
            </div>
          ) : null}
        </div>
        {browseExpirementsMode && (
          <div style={{ flex: 1, minHeight: 0 }}>
            <iframe
              src={dbUiUrl}
              style={{ width: "100%", height: "100%", border: "none", display: "block" }}
              title="Expirements DB User Interface"
            ></iframe>
          </div>
        )}
        {!browseExpirementsMode && !inputMode ? (
          <div
            style={{
              paddingLeft: "10px",
              display: "flex",
              width: "100%",
              textAlign: "left",
            }}
          >
            {schemaValidity === true ? (
              <>
                <div
                  style={{
                    paddingRight: "10px",
                    display: "flex",
                    justifyContent: "left",
                    alignItems: "center",
                    color: "green",
                  }}
                >
                  {schemaMessage}. You can now render the form.
                </div>
                <Button
                  style={{ marginRight: "5px" }}
                  onClick={() => renderOnClick()}
                  variant="outlined"
                >
                  Render
                </Button>
                <Button
                  style={{ marginRight: "10px" }}
                  onClick={() => clearSchemaOnClick()}
                  variant="outlined"
                  color="secondary"
                >
                  Clear
                </Button>
              </>
            ) : (
              <>
                <div
                  style={{
                    paddingRight: "10px",
                    paddingTop: "10px",
                    display: "flex",
                    justifyContent: "center",
                    alignItems: "center",
                    color: "red",
                  }}
                >
                  {schemaMessage}
                </div>
              </>
            )}
            {createScratchMode === true ? (
              <>
                <div
                  style={{
                    paddingRight: "10px",
                    display: "flex",
                    justifyContent: "center",
                    alignItems: "center",
                    color: "green",
                  }}
                >
                  Create from scratch mode. You can now start editing.
                </div>
                <Button
                  onClick={() => clearSchemaOnClick()}
                  variant="outlined"
                  color="secondary"
                >
                  Clear
                </Button>
              </>
            ) : null}
          </div>
        ) : null}
        {!browseExpirementsMode && (
          <div style={{ padding: "10px" }}>
            <Divider />
          </div>
        )}
        {renderReady === true ? (
          <FormRenderer
            revertAllChanges={revertAllChanges}
            schema={convertedSchema}
            setSchemaSpecification={setSchemaSpecification}
            originalSchema={schema}
            edit={editMode}
          />
        ) : null}
        {!browseExpirementsMode && (
          <>
            <div style={{ padding: "10px" }}>
              <Divider />
            </div>
            <div
              style={{
                padding: "10px 10px",
                display: "flex",
                justifyContent: "right",
              }}
            >
              {inputMode ? (
                <div style={{ width: "100%", display: "inline-block" }}>
                  <Button
                    onClick={() => toEditMode()}
                    style={{ float: "left", marginRight: "5px" }}
                    variant="outlined"
                  >
                    Back to Edit Mode
                  </Button>
                  {createScratchMode || browseSchemaMode === true ? (
                  <>
                    <Button
                      onClick={() => handleOnClickSaveSchema()}
                      style={{ float: "right" }}
                      variant="contained"
                      color="primary"
                    >
                      Save Schema
                    </Button>
                  </>
                  ) : null}
	              {createScratchMode || browseSchemaMode === false ? (
                  	<>
                      <Button
                        onClick={() => handleDownloadJsonSchema()}
                        style={{ float: "right" }}
                        variant="outlined"
                      >
                       <DownloadIcon /> Download Schema
                      </Button>
                      <Button
                        onClick={() => handleDownloadFormData()}
                        style={{ float: "right", marginRight: "5px" }}
                        variant="outlined"
                      >
                        <DownloadIcon /> Download Dataset
                      </Button>
                    </>
                  ) : null} 
                </div>
              ) : (
                <Button
                  disabled={disable}
                  onClick={compileOnClick}
                  variant="contained"
                  color="primary"
                >
                  Compile
                </Button>
              )}
            </div>
          </>
        )}
        {/* <div style={{ padding: "10px", color: "grey" }}>ADAMANT v1.2.0</div>
        <Button variant="contained" color="primary" onClick={handleLogin}>
        Login
        </Button> */}
        {!browseExpirementsMode && (
          <div style={{ padding: "10px", color: "grey" }}>
            <div>ADAMANT v1.2.0</div>
            <Button variant="contained" color="primary" onClick={handleLogout}>
              Logout
            </Button>
          </div>
        )}
      </div>
    </FormContext.Provider>
  );
};

export default AdamantMain;
