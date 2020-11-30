import React, { PropsWithChildren } from 'react';
import Button from "@material-ui/core/Button";
// import './components/Toolbar/rdg-toolbar.less';

interface Props {
  onAddRow: (arg: { newRowIndex: number }) => void;
  numberOfRows: number;
}

export default function Toolbar(props: PropsWithChildren<Props>) {
  function onAddRow() {
    props.onAddRow({ newRowIndex: props.numberOfRows });
  }

  return (
    <div className="rdg-toolbar">
      <div className="tools">
        <Button variant="outlined" color="primary" type="button" onClick={onAddRow}>Add Row</Button>
        {props.children}
      </div>
    </div>
  );
}
