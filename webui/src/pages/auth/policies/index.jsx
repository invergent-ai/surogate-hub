import React, {useEffect, useState} from "react";
import { useOutletContext } from "react-router-dom";
import Button from "react-bootstrap/Button";

import {auth} from "../../../lib/api";
import {useAPIWithPagination} from "../../../lib/hooks/api";
import {ConfirmationButton} from "../../../lib/components/modals";
import {Paginator} from "../../../lib/components/pagination";
import {PolicyEditor} from "../../../lib/components/policy";
import {
    ActionGroup,
    ActionsBar,
    Checkbox,
    DataTable,
    AlertError,
    FormattedDate,
    Loading,
    RefreshButton,
} from "../../../lib/components/controls";
import {useRouter} from "../../../lib/hooks/router";
import {Link} from "../../../lib/components/nav";
import { disallowPercentSign, INVALID_POLICY_ID_ERROR_MESSAGE } from "../validation";


const PoliciesContainer = () => {
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [refresh, setRefresh] = useState(false);
    const [createModalError, setCreateModalError] = useState(null);

    const router = useRouter();
    const after = (router.query.after) ? router.query.after : "";
    const { results, loading, error, nextPage } =  useAPIWithPagination(() => {
        return auth.listPolicies("", after);
    }, [after, refresh]);

    useEffect(() => { setSelected([]); }, [after, refresh]);

    if (error) return <AlertError error={error}/>;
    if (loading) return <Loading/>;

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button
                        variant="success"
                        onClick={() => setShowCreate(true)}>
                        Create Policy
                    </Button>

                    <ConfirmationButton
                        onConfirm={() => {
                            auth.deletePolicies(selected.map(p => p.id))
                                .catch(err => setDeleteError(err))
                                .then(() => {
                                    setSelected([]);
                                    setRefresh(!refresh);
                                });
                        }}
                        disabled={(selected.length === 0)}
                        variant="danger"
                        msg={`Are you sure you'd like to delete ${selected.length} policies?`}>
                        Delete Selected
                    </ConfirmationButton>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>
            <div className="auth-learn-more">
                A policy defines the permissions of a user or a group.
            </div>

            {(!!deleteError) && <AlertError error={deleteError}/>}

            <PolicyEditor
                onSubmit={(policyId, policyBody) => {
                    return auth.createPolicy(policyId, policyBody).then(() => {
                        setSelected([]);
                        setCreateModalError(null);
                        setShowCreate(false);
                        setRefresh(!refresh);
                    }).catch((err) => {
                        setCreateModalError(err.message);
                    })
                }}
                onHide={() => {
                    setCreateModalError(null);
                    setShowCreate(false)
                }}
                show={showCreate}
                validationFunction={disallowPercentSign(INVALID_POLICY_ID_ERROR_MESSAGE)}
                externalError={createModalError}
            />

            <DataTable
                results={results}
                headers={['', 'Policy ID', 'Created At']}
                keyFn={policy => policy.id}
                rowFn={policy => [
                    <Checkbox
                        name={policy.id}
                        onAdd={() => setSelected([...selected, policy])}
                        onRemove={() => setSelected(selected.filter(p => p !== policy))}
                    />,
                    <Link href={{pathname: '/auth/policies/:policyId', params: {policyId: policy.id}}}>
                        {policy.id}
                    </Link>,
                    <FormattedDate dateValue={policy.creation_date}/>
                ]}/>

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={after => router.push({pathname: '/auth/policies', query: {after}})}
            />
        </>
    );
};


const PoliciesPage = () => {
    const [setActiveTab] = useOutletContext();
    useEffect(() => setActiveTab("policies"), [setActiveTab]);
    return <PoliciesContainer/>;
};

export default PoliciesPage;
