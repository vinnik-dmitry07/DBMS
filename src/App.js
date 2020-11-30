import faker from 'faker';
import React, {useState, useMemo, useCallback} from 'react';
import DataGrid, {CalculatedColumn, Column, RowsUpdateEvent, SelectColumn} from 'react-data-grid';
import Toolbar from "./Toolbar";
import './AllFeatures.css';
import './style/index.css';
import 'react-checkbox-tree/lib/react-checkbox-tree.css';
import '@fortawesome/fontawesome-free/css/all.css'
import CheckboxTree from 'react-checkbox-tree';
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome'
import {faDatabase, faTable, faColumns, faPlus} from '@fortawesome/free-solid-svg-icons'

import Button from "@material-ui/core/Button";
import DialogTitle from "@material-ui/core/DialogTitle";
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import TextField from "@material-ui/core/TextField";
import DialogActions from "@material-ui/core/DialogActions";
import Dialog from "@material-ui/core/Dialog";


interface Row {
    id: string;
    avatar: string;
    email: string;
    title: string;
    firstName: string;
    lastName: string;
    street: string;
    zipCode: string;
    date: string;
    bs: string;
    catchPhrase: string;
    companyName: string;
    words: string;
    sentence: string;
}

faker.locale = 'en_GB';

function createFakeRowObjectData(index: number): Row {
    // noinspection JSUnresolvedVariable,JSUnresolvedFunction
    return {
        id: `id_${index}`,
        avatar: faker.image.avatar(),
        email: faker.internet.email(),
        title: faker.name.prefix(),
        firstName: faker.name.firstName(),
        lastName: faker.name.lastName(),
        street: faker.address.streetName(),
        zipCode: faker.address.zipCode(),
        date: faker.date.past().toLocaleDateString(),
        bs: faker.company.bs(),
        catchPhrase: faker.company.catchPhrase(),
        companyName: faker.company.companyName(),
        words: faker.lorem.words(),
        sentence: faker.lorem.sentence()
    };
}

function createRows(numberOfRows: number): Row[] {
    const rows: Row[] = [];

    for (let i = 0; i < numberOfRows; i++) {
        rows[i] = createFakeRowObjectData(i);
    }

    return rows;
}

function isAtBottom(event: React.UIEvent<HTMLDivElement>): boolean {
    const target: HTMLDivElement = event.target;
    return target.clientHeight + target.scrollTop === target.scrollHeight;
}

function loadMoreRows(newRowsCount: number, length: number): Promise<Row[]> {
    return new Promise(resolve => {
        const newRows: Row[] = [];

        for (let i = 0; i < newRowsCount; i++) {
            newRows[i] = createFakeRowObjectData(i + length);
        }

        setTimeout(() => resolve(newRows), 1000);
    });
}

let nodes = [
    {
        value: 'base1',
        label: 'Base1',
        icon: <FontAwesomeIcon icon={faDatabase}/>,
        children: [
            {
                value: 'table1',
                label: 'Table1',
                icon: <FontAwesomeIcon icon={faTable}/>,
                children: [
                    {value: 'column1', label: 'Column1', icon: <FontAwesomeIcon icon={faColumns}/>,},
                    {value: 'column2', label: 'Column2', icon: <FontAwesomeIcon icon={faColumns}/>,},
                    {value: 'column3', label: 'Column3', icon: <FontAwesomeIcon icon={faColumns}/>,},
                    {
                        value: '_add-column',
                        label: 'Add',
                        icon: <FontAwesomeIcon icon={faPlus}/>,
                        showCheckbox: false,
                        className: 'add'
                    },
                ],
            },
            {
                value: 'table2',
                label: 'Table2',
                icon: <FontAwesomeIcon icon={faTable}/>,
            },
            {
                value: '_add-table',
                label: 'Add',
                icon: <FontAwesomeIcon icon={faPlus}/>,
                showCheckbox: false,
                className: 'add'
            }
        ],
    },
    {
        value: '_add-base',
        label: 'Add',
        icon: <FontAwesomeIcon icon={faPlus}/>,
        showCheckbox: false,
        className: 'add'
    }
];

class Tree extends React.Component {
    state = {
        checked: [],
        expanded: [],
        addDialogOpen: false,
    };

    constructor() {
        super();
        this.handleAddDialogClose = this.handleAddDialogClose.bind(this)
    }

    _del(o, key) {
        for (const [i, child] of o.children.entries()) {
            if (child.value === key) {
                o.children.splice(i, 1);
                this.setState(prevState => ({
                    checked: prevState.checked.filter(check => check.value !== key)
                }));
            } else if (child.children) {
                this._del(child, key);
            }
        }
        if (o.value !== 'root' && o.children.length === 1) {
            this.del(o.value);
        }
    }

    del(key) {
        this._del({value: 'root', children: nodes}, key);
    }

    handleAddDialogClose() {
        this.setState({addDialogOpen: false});
    }

    render() {
        return (
            <>
                <Button variant="outlined" color="primary" onClick={() => {
                    for (const value of this.state.checked) {
                        if (!value.startsWith('_')) {
                            this.del(value);
                        }
                    }
                }}>
                    Delete
                </Button>

                <Dialog open={this.state.addDialogOpen}
                        onClose={this.handleAddDialogClose}
                        aria-labelledby="form-dialog-title">
                    <DialogTitle id="form-dialog-title">Adding...</DialogTitle>
                    <DialogContent>
                        <DialogContentText>
                            Enter name:
                        </DialogContentText>
                        <TextField
                            autoFocus
                            margin="dense"
                            id="name"
                            label="Name"
                            type="text"
                            fullWidth
                        />
                    </DialogContent>
                    <DialogActions>
                        <Button onClick={this.handleAddDialogClose} color="primary">
                            Cancel
                        </Button>
                        <Button onClick={this.handleAddDialogClose} color="primary">
                            Add
                        </Button>
                    </DialogActions>
                </Dialog>

                <CheckboxTree
                    nodes={nodes}
                    checked={this.state.checked}
                    expanded={this.state.expanded}
                    onCheck={checked =>
                        this.setState({checked})
                    }
                    onClick={
                        targetNode => {
                            if (targetNode.value.startsWith('_add')) {
                                console.log(targetNode.parent.value || 'root');
                                this.setState({addDialogOpen: true})
                            }
                        }
                    }
                    onExpand={expanded => this.setState({expanded})}
                    iconsClass="fa5"
                />
            </>
        );
    }
}

function SplitPane(props) {
    return (
        <div className="SplitPane">
            <div className="SplitPane-left">
                {props.left}
            </div>
            <div className="SplitPane-right">
                {props.right}
            </div>
        </div>
    );
}

export default function App() {
    return (
        <SplitPane
            left={
                <Tree/>
            }
            right={
                <Table/>
            }/>
    );
}

function Table() {
    const [rows, setRows] = useState(() => createRows(2));
    const [selectedRows, setSelectedRows] = useState(() => new Set());
    const [isLoading, setIsLoading] = useState(false);
    const gridRef = React.createRef();

    const columns = useMemo((): Column<Row>[] => [
        SelectColumn,
        {
            key: 'id',
            name: 'ID',
            width: 80,
            resizable: true,
            frozen: true
        },
        {
            key: 'avatar',
            name: 'Avatar',
            width: 40,
            resizable: true,
        },
        {
            key: 'title',
            name: 'Title',
            width: 200,
            resizable: true,
            formatter(props) {
                return <>{props.row.title}</>;
            }
        },
        {
            key: 'firstName',
            name: 'First Name',
            editable: true,
            width: 200,
            resizable: true,
            frozen: true
        },
        {
            key: 'lastName',
            name: 'Last Name',
            editable: true,
            width: 200,
            resizable: true,
            frozen: true
        },
        {
            key: 'email',
            name: 'Email',
            editable: true,
            width: 200,
            resizable: true
        },
        {
            key: 'street',
            name: 'Street',
            editable: true,
            width: 200,
            resizable: true
        },
        {
            key: 'zipCode',
            name: 'ZipCode',
            editable: true,
            width: 200,
            resizable: true
        },
        {
            key: 'date',
            name: 'Date',
            editable: true,
            width: 200,
            resizable: true
        },
        {
            key: 'bs',
            name: 'bs',
            editable: true,
            width: 200,
            resizable: true
        },
        {
            key: 'catchPhrase',
            name: 'Catch Phrase',
            editable: true,
            width: 200,
            resizable: true
        },
        {
            key: 'companyName',
            name: 'Company Name',
            editable: true,
            width: 200,
            resizable: true
        },
        {
            key: 'sentence',
            name: 'Sentence',
            editable: true,
            width: 200,
            resizable: true
        }
    ], []);

    const handleRowUpdate = useCallback(({fromRow, toRow, updated, action}: RowsUpdateEvent<Partial<Row>>): void => {
        const newRows = [...rows];
        let start: number;
        let end: number;

        if (action === 'COPY_PASTE') {
            debugger
            start = toRow;
            end = toRow;
        } else {
            start = Math.min(fromRow, toRow);
            end = Math.max(fromRow, toRow);
        }

        for (let i = start; i <= end; i++) {
            newRows[i] = {...newRows[i], ...updated};
        }

        setRows(newRows);
    }, [rows]);

    const handleAddRow = useCallback(({newRowIndex}: { newRowIndex: number }): void =>
        setRows([...rows, createFakeRowObjectData(newRowIndex)]), [rows]);

    const handleRowClick = useCallback((rowIdx: number, row: Row, column: CalculatedColumn<Row>) => {
        if (column.key === 'title') {
            debugger
            gridRef.current === null || gridRef.current.selectCell({rowIdx, idx: column.idx}, true);
        }
    }, []);

    async function handleScroll(event: React.UIEvent<HTMLDivElement>) {
        if (!isAtBottom(event)) return;

        setIsLoading(true);

        const newRows = await loadMoreRows(50, rows.length);

        setRows([...rows, ...newRows]);
        setIsLoading(false);
    }

    return (
        <div className="all-features">
            <Toolbar onAddRow={handleAddRow} numberOfRows={rows.length}/>
            <DataGrid
                ref={gridRef}
                columns={columns}
                rows={rows}
                rowKey="id"
                onRowsUpdate={handleRowUpdate}
                onRowClick={handleRowClick}
                rowHeight={30}
                selectedRows={selectedRows}
                onScroll={handleScroll}
                onSelectedRowsChange={setSelectedRows}
                rowClass={row => row.id.includes('7') ? 'highlight' : undefined}
                enableCellCopyPaste
                enableCellDragAndDrop
            />
            {isLoading && <div className="load-more-rows-tag">Loading more rows...</div>}
        </div>
    );
}
