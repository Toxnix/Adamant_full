import React from "react";
import Divider from '@mui/material/Divider';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import FileReadingGif from '../assets/fileloading.gif'
import { CircularProgress, LinearProgress } from "@mui/material";


const FilesDialog = ({ openFilesDialog, setOpenFilesDialog, content }) => {

    return (<>
        <Dialog
            onClose={() => openFilesDialog}
            open={openFilesDialog}
            width="500px"
            aria-labelledby="alert-dialog-title"
            aria-describedby="alert-dialog-description"
        >
            <DialogTitle id="alert-dialog-title">
                <div style={{ display: "inline-flex", width: "100%", verticalAlign: "middle" }}>
                    <div style={{ fontSize: "20px", width: "100%", alignSelf: "center" }}>
                        {content[0]}
                    </div>
                </div>
            </DialogTitle>
            <Divider />
            <DialogContent>
                <div style={{ width: "100%", justifyContent: "center" }}>
                    {/*<img
                        style={{ width: "300px", borderRadius: "5px" }}
                        alt="filereadingif"
                        src={FileReadingGif}
                    />*/}
                    <div style={{ display: "flex", justifyContent: "center" }}>
                        <CircularProgress size={"50pt"} />
                    </div>
                    <div style={{ fontSize: "15px", width: "100%", textAlign: "center" }}>
                        {content[1]}
                    </div>
                    <div style={{ fontSize: "15px", width: "100%", textAlign: "center" }}>
                        {content[2]}
                    </div>
                </div>
            </DialogContent>
            <DialogActions>
            </DialogActions>
        </Dialog>
    </>);
};

export default FilesDialog;