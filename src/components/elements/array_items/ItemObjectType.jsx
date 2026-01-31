import React, { useEffect, useState } from 'react'
import ElementRenderer from "../../ElementRenderer";
import Divider from '@mui/material/Divider';
import Accordion from "@mui/material/Accordion";
import { AccordionDetails, AccordionSummary } from '@mui/material';
import { IconButton } from '@mui/material';
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { Button } from '@mui/material';
import DeleteIcon from "@mui/icons-material/Delete";
import { FileIcon, defaultStyles } from 'react-file-icon'

const ItemObjectType = ({ path, dataInputItems, setDataInputItems, field_label, pathFormData, field_required, field_items, field_type, edit, index, field_key, handleDeleteArrayItem }) => {
    const [expand, setExpand] = useState(dataInputItems[index]["adamant-ui-specific-expand"] === undefined ? true : dataInputItems[index]["adamant-ui-specific-expand"]); // set to "true" for normally open accordion
    const [useIcon, setUseIcon] = useState(false);
    const [mimeType, setMimeType] = useState("");
    const [fileName, setFileName] = useState("")
    const [extension, setExtension] = useState("")
    let objectIndex = index;
    let field_properties = field_items["properties"]
    let withinArray = true;
    let withinObject = true;


    // This is to expand or contract the accordion, because normally open is used 
    const expandOnChange = () => {
        const value = expand
        setExpand(!value)

        let newVal = [...dataInputItems]
        newVal[index]["adamant-ui-specific-expand"] = !value

        setDataInputItems(newVal)
    };

    // Check if file keyword has some file there
    useEffect(() => {
        if (field_properties["file"] !== undefined) {
            if (field_properties["file"]["value"] !== undefined) {
                if (field_properties["file"]["value"] !== "") {
                    setUseIcon(true);
                    let something = field_properties["file"]["value"].split(";");
                    setFileName(something[1])
                    setMimeType(something[0].replace("fileupload:", ""))//.replace("data:", ""));
                    let ext = something[1].split(".")
                    setExtension(ext.slice(-1)[0])
                }
            }
        }
    }, [field_items])


    //const classes = useStyles();

    return (<>
        <div style={{ width: "100%", padding: "10px 0px 10px 0px" }}>
            <Accordion expanded={expand} >
                <AccordionSummary
                    expandIcon={
                        <IconButton
                            onClick={(event) => {
                                event.stopPropagation();
                                expandOnChange();
                            }}
                        >
                            <ExpandMoreIcon />
                        </IconButton>
                    }
                    style={{ verticalAlign: "middle", height: "auto" }}
                    aria-controls="panel1a-content"
                    id="panel1a-header"
                >
                    <div style={{ paddingTop: "10px", paddingBottom: "10px", display: 'inline-flex', width: '100%' }}>
                        <div style={{ lineHeight: "40px", height: "40px", width: "100%", verticalAlign: "middle" }} >
                            <strong>{field_label + " #" + parseInt(index + 1)}</strong>
                        </div>
                        {useIcon && !expand ? <div style={{ display: "flex", lineHeight: "40px", height: "40px", width: "100%", verticalAlign: "middle" }}>
                            {/*<img style={{ paddingRight: "10px" }} alt='fileIcon' src={FileIconx} height="40px" />*/}
                            <div style={{ display: "inline-flex", lineHeight: "40px", height: "40px", width: "100%", verticalAlign: "middle" }}>
                                <div style={{ display: "flex", lineHeight: "40px", height: "40px", width: "10%" }}>
                                    <FileIcon size={48} extension={extension} {...defaultStyles[extension]} />
                                </div>
                                <div style={{ width: "50%", whiteSpace: "nowrap", textOverflow: "ellipsis", overflow: "hidden", }}>
                                    {fileName.slice(0, 20) + " | " + mimeType}
                                </div>
                            </div>
                        </div> : <div style={{ width: "100%" }}> </div>}
                        {edit ? <Button onClick={() => handleDeleteArrayItem(index)} style={{ marginLeft: "5px" }}><DeleteIcon color="secondary" /></Button> : null}
                    </div>
                </AccordionSummary>
                <Divider />
                <AccordionDetails>
                    <div style={{ width: "100%" }}>
                        {Object.keys(field_properties).map((item, index) => {
                            let tempField = JSON.parse(JSON.stringify(field_properties[item]));
                            if (tempField["enum"]) {
                                tempField["enumerate"] = tempField["enum"]
                            }
                            if (tempField["default"]) {
                                tempField["defaultValue"] = tempField["default"]
                            }
                            return (
                                <div key={item} style={{ display: "flex" }}>
                                    <ElementRenderer withinObject={withinObject} path={path} pathFormData={pathFormData} withinArray={withinArray} dataInputItems={dataInputItems} setDataInputItems={setDataInputItems} fieldkey={item} fieldIndex={objectIndex} elementRequired={field_items["required"]} edit={false} field={tempField} />
                                </div>
                            )
                        })
                        }
                    </div>
                </AccordionDetails>
            </Accordion>
        </div >
    </>);

};

export default ItemObjectType;
