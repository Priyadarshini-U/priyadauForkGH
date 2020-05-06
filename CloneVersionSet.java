package amazon.devtools.bmds.methods.versionsets;

import amazon.devtools.bmds.versionsets.VersionSetGroupImpl;
import com.amazon.coral.metrics.Metrics;
import com.amazon.devtools.bmds.generated.InvalidRequestException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.hibernate.SQLQuery;

import amazon.devtools.bmds.bean.VersionedBean;
import amazon.devtools.bmds.events.EventBean;
import amazon.devtools.bmds.events.EventImpl;
import amazon.devtools.bmds.flavors.VersionSetFlavorMapEntry;
import amazon.devtools.bmds.major_versions.VersionSetMajorVersionMapEntry;
import amazon.devtools.bmds.methods.EventedWritingMethod;
import amazon.devtools.bmds.methods.versionsets.permissions.DoesVersionSetTipHaveConsumptionRestrictedPackages;
import amazon.devtools.bmds.platforms.VersionSetPlatformMapEntry;
import amazon.devtools.bmds.service.HibernateUtil;
import amazon.devtools.bmds.service.exceptions.BMDSException;
import amazon.devtools.bmds.service.exceptions.ClientException;
import amazon.devtools.bmds.service.exceptions.PersistenceException;
import amazon.devtools.bmds.versionsets.VersionSetBean;
import amazon.devtools.bmds.versionsets.VersionSetDetailsBean;
import amazon.devtools.bmds.versionsets.VersionSetFactory;
import amazon.devtools.bmds.versionsets.VersionSetImpl;
import amazon.devtools.bmds.versionsets.VersionSetLockBean;
import amazon.devtools.bmds.versionsets.VersionSetPruneableMajorVersionMapEntry;
import amazon.devtools.bmds.versionsets.revision.VersionSetRevisionBean;
import amazon.devtools.bmds.versionsets.revision.VersionSetRevisionImpl;
import amazon.devtools.bmds.versionsets.revision.WrappedVersionSetRevisionBean;
import amazon.platform.profiler.Profiler;
import amazon.platform.profiler.ProfilerScope;

/**
 * Clones a version set into a new version of another (possibly
 * to-be-created-in-this-call) version set.
 *
 * @author aaronson
 */
public class CloneVersionSet extends
    EventedWritingMethod<VersionSetImpl> {

    private String _sourceVersionSetName;
    private Long   _sourceEventId;
    private Integer _sourceRevision;
    private String _targetVersionSetName;
    private String _p4group;
    private boolean _overwrite;
    private EventImpl _event;
    private HibernateUtil _util;
    private VersionSetGroupImpl versionSetGroup;
    private String _bindleId;

    private final Timestamp expirationTime;
    private final Metrics _metrics;

    public CloneVersionSet(String sourceVersionSetName,
                           Long sourceEventId,
                           Integer sourceRevision,
                           String targetVersionSetName,
                           String p4group,
                           boolean overwrite,
                           VersionSetGroupImpl versionSetGroup,
                           String bindleId,
                           Metrics metrics) {
        _sourceVersionSetName = sourceVersionSetName;
        _sourceEventId = sourceEventId;
        _sourceRevision = sourceRevision;
        _targetVersionSetName = targetVersionSetName;
        _p4group = (p4group == null || p4group.equals("")) ? null : p4group;
        _overwrite = overwrite;
        this.versionSetGroup = versionSetGroup;
        _bindleId = bindleId;

        _util = HibernateUtil.getInstance();

        expirationTime = ExtendVersionSetExpiration.calculateNewExpirationTimestamp(30);
        _metrics = metrics;
    }

    private void flush() {
        _util.getSession().flush();
    }

    public VersionSetImpl run(EventImpl event) throws BMDSException,PersistenceException {
        _event = event;
        VersionSetFactory vsf = new VersionSetFactory();

        GetVersionSetByName findVs = new GetVersionSetByName(_targetVersionSetName);
        VersionSetImpl vs = findVs.run();

        boolean isRecreate = vs != null;
        if (vs != null && !vs.getBean().isDeprecated() && !_overwrite) {
            throw new ClientException("Version set " + _targetVersionSetName + " already exists.  "
                    + "Specify 'overwrite' to force overwriting the existing version set.");
        }
        Long destVersionSetEventId = null;
        if (isRecreate) {
           destVersionSetEventId = VersionSetFactory.tipEventForVersionSet(vs);
        }
        GetVersionSetByName findFromVs = new GetVersionSetByName(_sourceVersionSetName);
        VersionSetImpl fromVersionSet = findFromVs.run();
        if (fromVersionSet == null) {
            throw new ClientException("Attempt to clone from non-existent version set: " + _sourceVersionSetName);
        }

        Long fromEventId = normalizeSourceEventId(vsf, fromVersionSet);
        fromVersionSet.getBean().setRevisionEventId(fromEventId);

        VersionSetImpl targetVersionSet = setupNewTargetVersionSetDetails(fromVersionSet,
                                                                          _targetVersionSetName,
                                                                          event.getUserLogin(),
                                                                          _p4group,
                                                                          versionSetGroup);
        flush();
        updateTargets(fromVersionSet, targetVersionSet);
        updatePackageVersions(fromVersionSet, targetVersionSet);
        updateFlavors(fromVersionSet, targetVersionSet);
        updatePlatforms(fromVersionSet, targetVersionSet);
        updatePruneableList(fromVersionSet, targetVersionSet);

        _util.getSession().save(targetVersionSet.getBean());
        _util.getSession().save(targetVersionSet.getBean().getDetails());
        flush();
        _util.getSession().refresh(targetVersionSet.getBean());
        updateMajorVersionSources(fromVersionSet, targetVersionSet);
        new DoesVersionSetTipHaveConsumptionRestrictedPackages(_targetVersionSetName, destVersionSetEventId).run();
        new HandleNewVsRevision(targetVersionSet.getBean(), event.getBean()).run();

        return targetVersionSet;
    }

    private long normalizeSourceEventId(VersionSetFactory vsf, VersionSetImpl fromVersionSet)
            throws PersistenceException, ClientException {
        long fromEventId;
        if (_sourceRevision == null) {
            if (_sourceEventId != null) {
                // if it didn't occur on the fromVersionSet, we need to normalize it
                VersionSetRevisionBean fromRevision = vsf.getVersionSetRevision(fromVersionSet.getBean(),
                                                                                _sourceEventId);
                if(fromRevision == null) {
                    throw new InvalidRequestException("No version set revision exists for " + fromVersionSet.getName() + "@" + _sourceEventId + ". " +
                            "Also, event id " + _sourceEventId + " predates the creation of " + fromVersionSet.getName() + ", " +
                            "so it is not possible to normalize this request and return the nearest prior version set revision.");
                }
                fromEventId = _sourceEventId;
            } else {
                // No revision or eventId specified - we're going to use 'tip'.
                VersionSetRevisionImpl fromRevision = vsf.getRevisions(fromVersionSet.getBean(), null, null, 1).get(0);
                fromEventId = fromRevision.getEventId();
            }
        } else {
            WrappedVersionSetRevisionBean revision =
                vsf.getRevision(fromVersionSet.getBean(), _sourceRevision);

            if (revision == null || revision.getBean() == null) {
                throw new ClientException("Attempt to clone from non-existent version set revision: " +
                                          _sourceVersionSetName + "#" + _sourceRevision);
            }

            fromEventId = revision.getEventId();
        }
        return fromEventId;
    }

    /**
     * Clone from the sourceVersionSet to targetVersionSet, update database and document store.
     * @param sourceVersionSet version set to clone from
     *                         I am not using fromVersionSet here because there is a fromVersionSet which stands for
     *                         the FromVersionSet of the source version set in this method. Still naming it fromVersionSet
     *                         will cause ambiguity and be really confusing for readers.
     * @param targetVersionSetName version set name to clone to
     * @param requester person who request this cloning
     * @param p4group  perforce group
     * @param versionSetGroup the version set group to create the target version set in
     * @return target version set to clone to
     */
    private VersionSetImpl setupNewTargetVersionSetDetails(VersionSetImpl sourceVersionSet,
                                                   String targetVersionSetName,
                                                   String requester,
                                                   String p4group,
                                                   VersionSetGroupImpl versionSetGroup)
        throws PersistenceException, BMDSException {

        VersionSetFactory vsf = new VersionSetFactory();
        VersionSetImpl targetVersionSet = vsf.getVersionSetByName(targetVersionSetName);
        VersionSetImpl trackingVersionSet = vsf.getVersionSetByName(sourceVersionSet.getTrackingVersionSetName());
        final long sourceVsEventId = sourceVersionSet.getBean().getRevisionEventId();

        Timestamp newExpirationTime = ExtendVersionSetExpiration.calculateNewExpirationTimestamp(30);

        String vfiDependencyType = sourceVersionSet.getBean().getVfiDependencyType();
        
        if (targetVersionSet == null || targetVersionSet.getBean() == null) {
            Long fromVersionSetEventId = null;
            if (sourceVersionSet.getBean().getFromVersionSetEvent() != null) {
                fromVersionSetEventId =
                    sourceVersionSet.getBean().getFromVersionSetEvent().getEventId();
            }

            String fromVersionSetName = sourceVersionSet.getFromVersionSetName();
            if (fromVersionSetName == null
                || "".equals(fromVersionSetName)
                || fromVersionSetEventId == null
                || fromVersionSetEventId == VersionedBean.NO_LAST_EVENT_ID) {
                // we didn't find the correct from versionset
                // information...  we'll make the versionset look like
                // it came from the versionset we're cloning.
                fromVersionSetName = sourceVersionSet.getName();
                fromVersionSetEventId = sourceVersionSet.getEventId();
            }

            if (fromVersionSetName == null
                || "".equals(fromVersionSetName)
                || fromVersionSetEventId == null
                || fromVersionSetEventId == VersionedBean.NO_LAST_EVENT_ID) {
                throw new BMDSException("Cannot create a non-live versionset with no from version set!");
            }

            flush();
            targetVersionSet =
                (new CreateEmptyVersionSet(targetVersionSetName,
                                      fromVersionSetName,
                                      fromVersionSetEventId,
                                      sourceVersionSet.getTrackingVersionSetName(),
                                      requester,
                                      requester,
                                      requester,
                                      sourceVersionSet.getBean().getBrazilToolsVersion(),
                                      vfiDependencyType,
                                      newExpirationTime.getTime(),
                                      sourceVersionSet.getBean().isModified(),
                                      sourceVersionSet.getBean().getSupportsPartialPlatforms(),
                                      sourceVersionSet.getBean().getDetails().getPackageMasterOverride(),
                                      p4group, 
                                      versionSetGroup,
                                      _bindleId,
                                      _metrics)).run(_event);
            flush();
            // This CreateVersionSet call will blow away the sourceVersionSet's revisionEventId if the targetVersionSet's
            // fromVersionSetName is the same as the name of the sourceVersionSet.  We save it and restore it here.
            sourceVersionSet.getBean().setRevisionEventId(sourceVsEventId);
            VersionSetLockBean.getLock(targetVersionSet.getBean(), _event.getBean());
        } else {
            if (targetVersionSet.getVersionSetId() == sourceVersionSet.getVersionSetId()) {
                throw new ClientException("Unable to clone a version set onto itself.");
            }

            VersionSetLockBean.getLock(targetVersionSet.getBean(), _event.getBean());
            VersionSetDetailsBean oldDetails = targetVersionSet.getBean().getDetails();

            VersionSetBean targetFromVersionSet = sourceVersionSet.getBean().getFromVersionSet();
            if (targetFromVersionSet == null) {
                targetFromVersionSet = sourceVersionSet.getBean();
            }

            EventBean targetFromVersionSetEvent = sourceVersionSet.getBean().getFromVersionSetEvent();
            if (targetFromVersionSetEvent == null) {
                targetFromVersionSetEvent = _event.getBean();
            }
            VersionSetDetailsBean newDetails =
                new VersionSetDetailsBean(targetVersionSet.getBean(),
                                          targetFromVersionSet,
                                          targetFromVersionSetEvent,
                                          trackingVersionSet.getBean(),
                                          sourceVersionSet.getBean().getBrazilToolsVersion(),
                                          vfiDependencyType,
                                          sourceVersionSet.getBean().getRequester(),
                                          newExpirationTime,
                                          sourceVersionSet.getBean().isModified(),
                                          sourceVersionSet.getBean().getSupportsPartialPlatforms(),
                                          sourceVersionSet.getBean().getDetails().getPackageMasterOverride());

            oldDetails.setLastEvent(_event.getBean());
            _util.getSession().update(oldDetails);
            flush();

            newDetails.setFirstEvent(_event.getBean());
            _util.getSession().save(newDetails);

            targetVersionSet.getBean().setDetails(newDetails);
            targetVersionSet.getBean().setLastEventId(VersionedBean.NO_LAST_EVENT_ID);
        }

        return targetVersionSet;
    }

    private void updateTargets(VersionSetImpl fromVersionSet, VersionSetImpl toVersionSet)
        throws PersistenceException, BMDSException {
        Map<String,Long> fromTargets =
            normalizeMap(fromVersionSet.getBean().getTargetsByMajorVersionString());
        Map<String,Long> toTargets =
            normalizeMap(toVersionSet.getBean().getTargetsByMajorVersionString());

        // Look through all the "to" targets, removing any that aren't
        // present in the "from" set
        for (Map.Entry<String, Long> entry : toTargets.entrySet()) {
            String name = entry.getKey();
            // remove deprecated targets
            if (!fromTargets.containsKey(name)) {
                Long mapKey = entry.getValue();
                VersionSetMajorVersionMapEntry mapEntry =
                    (VersionSetMajorVersionMapEntry)
                      _util.getSession().load(VersionSetMajorVersionMapEntry.class,
                                              mapKey);
                mapEntry.setLastEvent(_event.getBean());
                _util.getSession().update(mapEntry);
            } else {
                fromTargets.remove(name);
            }
        }

        // Anything left in the "from" version set is a new target to add
        for (Long mapKey : fromTargets.values()) {
            VersionSetMajorVersionMapEntry oldEntry =
                (VersionSetMajorVersionMapEntry)
                _util.getSession().load(VersionSetMajorVersionMapEntry.class,
                                        mapKey);

            VersionSetMajorVersionMapEntry newEntry =
                new VersionSetMajorVersionMapEntry(toVersionSet.getBean(),
                                                   oldEntry.getMajorVersion());
            newEntry.setFirstEvent(_event.getBean());
            _util.getSession().save(newEntry);
        }
        flush();
    }

    /**
     * Updates toVsImpl to contain exactly the package versions in fromVsImpl
     * @param fromVsImpl the Version set to duplicate; expects the eventId to be non-null and positive
     * @param toVsImpl the version set to copy into
     */
    private void updatePackageVersions(VersionSetImpl fromVsImpl, VersionSetImpl toVsImpl) throws PersistenceException {
        final String deletePVSql =
            "UPDATE /* CloneVersionSet.deletePackageVersions */\n" +
            "  vs_pv_map SET last_event_id = :eventId\n" +
            "WHERE last_event_id = -1 AND\n" +
            "      vs_id = :toVsId AND\n" +
            "      pv_id NOT IN (\n" +
            "  SELECT pvm.pv_id\n" +
            "    FROM vs_pv_map pvm\n" +
            "   WHERE pvm.vs_id = :fromVsId\n" +
            "     AND pvm.first_event_id <= :fromVsEventId\n" +
            "     AND (pvm.last_event_id > :fromVsEventId OR pvm.last_event_id = -1)\n" +
            "  )";
        ProfilerScope scope = Profiler.scopeStart("CloneVersionSet.deletePackageVersions");
        try {
            SQLQuery deletePVQuery = _util.getSession().createSQLQuery(deletePVSql);
            deletePVQuery.setLong("fromVsId", fromVsImpl.getVersionSetId());
            deletePVQuery.setLong("toVsId", toVsImpl.getVersionSetId());
            deletePVQuery.setLong("fromVsEventId", fromVsImpl.getEventId());
            deletePVQuery.setLong("eventId", _event.getEventId());
            deletePVQuery.executeUpdate();
        } finally {
            Profiler.scopeEnd(scope);
        }

        final String createPVSql =
            "INSERT /* CloneVersionSet.insertPackageVersions */\n" +
            "  INTO vs_pv_map (vs_pv_map_id, first_event_id, last_event_id, mv_id, pv_id, vs_id)\n" +
            "  SELECT " + HibernateUtil.getInstance().getDialect().getSelectSequenceNextValString("vs_pv_map_seq") +
            ", :eventId, -1, mv_id, pv_id, :toVsId\n" +
            "    FROM (\n" +
            // This subquery is a little non-standard; the reasoning is that it gets Oracle to run the
            // subsequent NOT IN subquery once, after doing the UNION ALL.  Otherwise, if it
            // were to be replaced with the usual (last_event_id > :fromVsEventId OR last_event_id = -1)
            // it performs the HASH JOIN ANTI against both parts (the > and the =-1) before concatenating,
            // requiring an extra INDEX RANGE SCAN and nearly 4 times the consistent gets overall.
            "      SELECT vs_id, first_event_id, mv_id, pv_id\n" +
            "        FROM vs_pv_map\n" +
            "       WHERE last_event_id > :fromVsEventId\n" +
            "    UNION ALL\n" +
            "      SELECT vs_id, first_event_id, mv_id, pv_id\n" +
            "        FROM vs_pv_map\n" +
            "       WHERE last_event_id = -1\n" +
            "    ) pvm\n" +
            "       WHERE pvm.vs_id = :fromVsId\n" +
            "         AND pvm.first_event_id <= :fromVsEventId\n" +
            "         AND pvm.pv_id NOT IN (SELECT pv_id FROM vs_pv_map WHERE vs_id = :toVsId AND last_event_id = -1)";
        scope = Profiler.scopeStart("CloneVersionSet.insertPackageVersions");
        try {
            SQLQuery createPVQuery = _util.getSession().createSQLQuery(createPVSql);
            createPVQuery.setLong("fromVsId", fromVsImpl.getVersionSetId());
            createPVQuery.setLong("toVsId", toVsImpl.getVersionSetId());
            createPVQuery.setLong("fromVsEventId", fromVsImpl.getEventId());
            createPVQuery.setLong("eventId", _event.getEventId());
            createPVQuery.executeUpdate();
        } finally {
            Profiler.scopeEnd(scope);
        }
    }

    private void updateFlavors(VersionSetImpl fromVersionSet, VersionSetImpl toVersionSet)
        throws PersistenceException, BMDSException {
        Map<String,Long> fromFlavors =
            normalizeMap(fromVersionSet.getBean().getFlavorsByName());
        Map<String,Long> toFlavors =
            normalizeMap(toVersionSet.getBean().getFlavorsByName());

        // Look through all the "to" flavors, removing any that aren't
        // present in the "from" set
        for (Map.Entry<String, Long> entry : toFlavors.entrySet()) {
            String name = entry.getKey();
            // remove flavors
            if (!fromFlavors.containsKey(name)) {
                Long mapKey = entry.getValue();
                VersionSetFlavorMapEntry mapEntry =
                    (VersionSetFlavorMapEntry)
                      _util.getSession().load(VersionSetFlavorMapEntry.class,
                                              mapKey);
                mapEntry.setLastEvent(_event.getBean());
                _util.getSession().update(mapEntry);
            } else {
                fromFlavors.remove(name);
            }
        }

        // Anything left in the "from" flavors set is a new flavor to
        // add
        for (Long mapKey : fromFlavors.values()) {
            VersionSetFlavorMapEntry oldEntry =
                (VersionSetFlavorMapEntry)
                _util.getSession().load(VersionSetFlavorMapEntry.class,
                                        mapKey);

            VersionSetFlavorMapEntry newEntry =
                new VersionSetFlavorMapEntry(oldEntry.getFlavor(),
                                             toVersionSet.getBean());
            newEntry.setFirstEvent(_event.getBean());
            _util.getSession().save(newEntry);
        }
        flush();
    }

    private void updatePlatforms(VersionSetImpl fromVersionSet, VersionSetImpl toVersionSet)
        throws PersistenceException, BMDSException {
        Map<String,Long> fromPlatforms =
            normalizeMap(fromVersionSet.getBean().getPlatformsByName());
        Map<String,Long> toPlatforms =
            normalizeMap(toVersionSet.getBean().getPlatformsByName());

        // Look through all the "to" platforms, removing any that
        // aren't present in the "from" set
        for (Map.Entry<String,Long> entry : toPlatforms.entrySet()) {
            String name = entry.getKey();
            // remove missing platforms
            if (!fromPlatforms.containsKey(name)) {
                Long mapKey = entry.getValue();
                VersionSetPlatformMapEntry mapEntry =
                    (VersionSetPlatformMapEntry)
                      _util.getSession().load(VersionSetPlatformMapEntry.class,
                                              mapKey);
                mapEntry.setLastEvent(_event.getBean());
                _util.getSession().update(mapEntry);
            } else {
                fromPlatforms.remove(name);
            }
        }

        // Anything left in the "from" platforms set is a new flavor to
        // add
        for (Long mapKey : fromPlatforms.values()) {
            VersionSetPlatformMapEntry oldEntry =
                (VersionSetPlatformMapEntry)
                _util.getSession().load(VersionSetPlatformMapEntry.class,
                                        mapKey);

            VersionSetPlatformMapEntry newEntry =
                new VersionSetPlatformMapEntry(toVersionSet.getBean(),
                                               oldEntry.getPlatform());
            newEntry.setFirstEvent(_event.getBean());
            _util.getSession().save(newEntry);
        }
        flush();
    }

    private void updatePruneableList(VersionSetImpl fromVersionSet, VersionSetImpl toVersionSet)
        throws PersistenceException, BMDSException {
        VersionSetFactory factory = new VersionSetFactory();

        // Remove all pruned major versions in the destination versionset
        for (VersionSetPruneableMajorVersionMapEntry mapEntry : factory.getPruneableMajorVersions(toVersionSet.getBean())) {
            mapEntry.setLastEvent(_event.getBean());
            _util.getSession().update(mapEntry);
        }

        Collection<VersionSetPruneableMajorVersionMapEntry> fromPruneablePackages =
                factory.getPruneableMajorVersions(fromVersionSet.getBean()).stream()
                        .collect(Collectors.toMap(
                                // key function - unique per major version
                                VersionSetPruneableMajorVersionMapEntry::getMajorVersion,
                                // value function - identity
                                Function.identity(),
                                // Historically, there can be duplicate prunes because older versions of recreateVersionSet
                                // updates lasteventId on all prune records for a versionset (not just tip).  This rewrote history.
                                // We need to pick the one that's most likely correct, which would be the later first event ID.
                                // In the rare case that there's multiple with the same event (say the source was itself reverted back)
                                // pick the one with the higher request ID.
                                BinaryOperator.maxBy(Comparator.comparingLong(VersionSetPruneableMajorVersionMapEntry::getFirstEventId)
                                        .thenComparingLong(VersionSetPruneableMajorVersionMapEntry::getRequestId))
                        )).values();

        // add back all of the new pruned major versions from the source versionset. This could include
        // some of the major versions we just removed, but we're doing this on purpose so that all first_event_ids
        // are changed to the clone event_id & date. This doesn't affect the correctness of the evented-storage model,
        // but simplifies the document storage model because it won't need to pull the current tip revision of the destination
        // to know which events need to be reused.
        for (VersionSetPruneableMajorVersionMapEntry oldEntry : fromPruneablePackages) {
            VersionSetPruneableMajorVersionMapEntry newEntry =
                new VersionSetPruneableMajorVersionMapEntry(toVersionSet.getBean(),
                                                            oldEntry.getMajorVersion());
            newEntry.setRequestId(oldEntry.getRequestId());
            newEntry.setFirstEvent(_event.getBean());
            _util.getSession().save(newEntry);
        }
        flush();
    }

    /**
     * Make a SQL query name comment from the given operation.
     *
     * @param operation
     *             The operation to make the query name from.
     *
     * @return a SQL comment string
     */
    private String makeQueryName(String operation) {
        return "/*CloneVersionSet." + operation + "*/ ";
    }

    /**
     * Start a ProfilerScope for the given operation.
     *
     * @param operation
     *             The operation to start a ProfilerScope for.
     *
     * @return the ProfilerScope object
     */
    private ProfilerScope scopeStart(String operation) {
        return Profiler.scopeStart("CloneVersionSet." + operation);
    }

    /**
     * Update the major version sources for the target version set based on the source version set.
     *
     * @param fromVersionSet
     *             The source version set.
     * @param toVersionSet
     *             The target version set.
     */
    private void updateMajorVersionSources(VersionSetImpl fromVersionSet, VersionSetImpl toVersionSet)
        throws PersistenceException
    {
        setLastEventOnMajorVersionSources(toVersionSet);
        copyMajorVersionSources(fromVersionSet, toVersionSet);
    }

    /**
     * Set the last event id of the major version sources for the given version set to the current event.
     *
     * @param toVersionSet
     *             The target version set.
     */
    private void setLastEventOnMajorVersionSources(VersionSetImpl toVersionSet) throws PersistenceException {
        final String setLastEventSql = "update "
            + makeQueryName("setLastEventOnMVSources")
            + "vs_mv_sources set last_event_id = :eventId "
            + "where vs_id = :versionSetId "
            + "and last_event_id = -1";

        ProfilerScope scope = scopeStart("setLastEventOnMVSources");
        try {
            SQLQuery query = _util.getSession().createSQLQuery(setLastEventSql);
            query.setLong("eventId", _event.getEventId());
            query.setLong("versionSetId", toVersionSet.getVersionSetId());
            query.executeUpdate();
        } finally {
            Profiler.scopeEnd(scope);
        }
    }

    /**
     * Copy the major version sources from the source version set to the target version set.
     *
     * @param fromVersionSet
     *             The source version set.
     * @param toVersionSet
     *             The target version set.
     */
    private void copyMajorVersionSources(VersionSetImpl fromVersionSet, VersionSetImpl toVersionSet)
        throws PersistenceException
    {
        String sequence;
        if (HibernateUtil.getInstance().isOracleDialect()) {
            sequence = "vs_mv_source_seq.nextval, ";
        } else {
            sequence = "next value for vs_mv_source_seq, ";
        }

        String copySql = "insert "
            + makeQueryName("copyMVSources")
            + "into vs_mv_sources "
            + "(vs_mv_source_id, vs_id, mv_id, source_vs_revision_id, source_pv_id, first_event_id, last_event_id) "
            + "select " + sequence
            + ":toVersionSetId, " // vs_id
            + "mv_id, " // mv_id
            + "source_vs_revision_id, " // source_vs_revision_id
            + "source_pv_id, " // source_pv_id
            + ":eventId, " // first_event_id
            + "-1 " // last_event_id
            + "from vs_mv_sources "
            + "where vs_id = :fromVersionSetId ";

        boolean useTip = (fromVersionSet.getEventId() == null ||
                          fromVersionSet.getEventId() == VersionedBean.NO_LAST_EVENT_ID);

        if (useTip) {
            copySql += "and last_event_id = -1";
        } else {
            copySql += "and first_event_id <= :fromVersionSetEventId " +
                       "and (last_event_id = -1 or last_event_id > :fromVersionSetEventId)";
        }

        ProfilerScope scope = scopeStart("copyMVSources");
        try {
            SQLQuery query = _util.getSession().createSQLQuery(copySql);
            query.setLong("toVersionSetId", toVersionSet.getVersionSetId());
            query.setLong("eventId", _event.getEventId());
            query.setLong("fromVersionSetId", fromVersionSet.getVersionSetId());

            if (!useTip) {
                query.setLong("fromVersionSetEventId", fromVersionSet.getEventId());
            }

            query.executeUpdate();
        } finally {
            Profiler.scopeEnd(scope);
        }
    }

    /**
     * Turn a map (from the VersionSetBean) into something that we're
     * more comfortable working with.  This will replace
     * <code>null</code> with an empty hash.  It will also copy the
     * hash that we're given, so that it can be modified during some
     * of the iterations over it (if we happen to be working with the
     * same source and target version set).
     */
    private Map<String,Long> normalizeMap(Map<String,Long> m) {
        if (m == null)
            return new HashMap<>();
        return new HashMap<>(m);
    }
}
 
