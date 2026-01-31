import React, { useCallback, useContext, useEffect, useState } from 'react'
import TextField from "@mui/material/TextField"
import { makeStyles } from '@mui/styles';
import Autocomplete from "@mui/material/Autocomplete";
import { Button } from '@mui/material';
import Radio from '@mui/material/Radio'
import RadioGroup from '@mui/material/RadioGroup';
import FormControlLabel from '@mui/material/FormControlLabel';

const useStyles = makeStyles((theme) => ({
    root: {
        width: 'auto',
    },
    heading: {
        fontSize: theme.typography.pxToRem(15),
        fontWeight: theme.typography.fontWeightRegular,
    },
}));

const style = {
    paddingTop: "10px",
    paddingBottom: "10px",
}

const StringUITypes = ({ }) => {
    const [multiChips, setMultiChips] = useState([])
    const [widgetType, setWidgetType] = useState('Text')
    const [value, setValue] = useState('Text')
    const [inputList, setInputList] = useState('')


    const handleRadioChange = (event) => {
        setValue(event.target.value);
    }

    const handleOnChangeListField = (event) => {
        setInputList(event.target.value);
    }

    return (
        <div>
            <RadioGroup row aria-label="widgetType" name="row-radio-buttons-group" value={value}
                onChange={handleRadioChange}>
                <FormControlLabel value="Text" control={<Radio />} label="Text" />
                <FormControlLabel value="Long Text" control={<Radio />} label="Long Text" />
                <FormControlLabel value="List" control={<Radio />} label="List" />
                <FormControlLabel value="Autocomplete" control={<Radio disabled />} label="Autocomplete" />
                <FormControlLabel value="Tag-like" control={<Radio disabled />} label="Tag-like" />
            </RadioGroup>
        </div>

    )
};

export default StringUITypes;