import React from "react";
import Divider from '@mui/material/Divider';
import { TextField } from '@mui/material';
import { IconButton } from '@mui/material';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import CloseIcon from '@mui/icons-material/Close';


const JSONSchemaViewerDialog = ({ openSchemaViewer, setOpenSchemaViewer, jsonschema }) => {

    return (<>
        <Dialog
            onClose={() => setOpenSchemaViewer(false)}
            maxWidth="md"
            fullWidth={true}
            open={openSchemaViewer}
            aria-labelledby="alert-dialog-title"
            aria-describedby="alert-dialog-description"
        >
            <DialogTitle id="alert-dialog-title">
                <div style={{ display: "inline-flex", width: "100%", verticalAlign: "middle" }}>
                    <div style={{ fontSize: "30px", width: "100%", alignSelf: "center" }}>
                        JSON Schema viewer
                    </div>
                    <IconButton onClick={() => setOpenSchemaViewer(false)}><CloseIcon fontSize="large" color="secondary" /></IconButton>
                </div>
            </DialogTitle>
            <Divider />
            <DialogContent>
                <TextField
                    disabled
                    fullWidth={true}
                    variant="filled"
                    multiline
                    defaultValue={JSON.stringify(jsonschema, null, 2)}
                />
            </DialogContent>
            <DialogActions>
            </DialogActions>
        </Dialog>

        {/*
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
                    aria-controls="panel1a-content"
                    id="panel1a-header"
                >
                    <div style={{ paddingTop: "10px", paddingBottom: "10px", display: 'inline-flex', width: '100%' }}>
                        <div>
                            <Typography className={classes.heading}>JSON Schema Viewer</Typography>
                        </div>
                    </div>
                </AccordionSummary>
                <Divider />
                <AccordionDetails>
                    <div >
                        <pre style={{ overflowX: "auto", whiteSpace: "pre-wrap", wordWrap: "break-word" }}>
                            {JSON.stringify(jsonschema, null, 2)}
                        </pre>
                    </div>
                </AccordionDetails>
            </Accordion>
        </div>
                */}
    </>);
};

export default JSONSchemaViewerDialog;
