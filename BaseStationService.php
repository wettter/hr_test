<?php


namespace app\modules\hot\services;

use app\modules\hot\dictionary\HbFeatureDictionary;
use app\modules\hot\dictionary\HotDictionary;
use bezlimit\components\base\BlService;
use bezlimit\models\entity\geo\BaseStationEntity;
use bezlimit\models\entity\phone\OperatorParamsEntity;
use bezlimit\models\entity\PhoneEntity;
use DateInterval;
use DateTime;
use Exception;
use Yii;
use yii\db\Expression;

/**
 * Class BaseStationService
 * @package app\modules\hot\services
 */
class BaseStationService extends BlService
{
    /** @var string кеш массива всех базовых станций  */
    public const BASE_STATIONS_CACHE_KEY = 'bese_stations_array_num_with_region_id';

    /** @var int регион Россия если не удалось определить регион по базовой станции  */
    public const BASE_STATIONS_REGION_RUSSIA = 1000;

    /**
     * Массив номер базовой станции => регион
     * @return false|mixed
     */
    public function getArrayNumWithRegionId()
    {
        $key = self::BASE_STATIONS_CACHE_KEY;

        if ($result = Yii::$app->cache->get($key)) {
            return $result;
        }

        $baseStations = BaseStationEntity::find()
            ->select(['base_station_id', 'region_id'])
            ->all();

        foreach ($baseStations as $baseStation) {
            $result[$baseStation->base_station_id] = $baseStation->region_id;
        }

        if (count($result) > 0) {
            Yii::$app->cache->set($key, $result, 86400);
        }

        return $result;
    }

    /**
     * @param array $phoneUnBilledCallsArray - результат от @see FileService::getInstance()->getHotFromCsvFile()
     * @param int $phone_number
     * @throws Exception
     */
    public function processing(array $phoneUnBilledCallsArray, int $phoneNumber)
    {

        $baseStationsArray = $this->getArrayNumWithRegionId();
        $phoneBaseStations = [];

        foreach ($phoneUnBilledCallsArray as $traffic_type => $phoneUnBilledCalls) {
            foreach ($phoneUnBilledCalls as $ctn => $phoneUnBilledCallsUniq) {
                $phone = PhoneEntity::findByPhone(substr($ctn, -10));
                if ($phone === null || $phone->faCount > 0) {
                    continue;
                }
                foreach ($phoneUnBilledCallsUniq as $uniqKey => $phoneUnBilledCall) {
                    //если номер базовой станции меньше 6 цифр или трафик LTEFREE - пропускаем
                    if (strlen($phoneUnBilledCall->cell_id) < 5
                        || $phoneUnBilledCall->callType === 'Трафик по LTEFREE'
                    ) {
                        continue;
                    }

                    $phoneNumber = $phone->phone;

                    $regionId = $baseStationsArray[$phoneUnBilledCall->cell_id]
                        ?? self::BASE_STATIONS_REGION_RUSSIA;

                    $callDate = DateTime::createFromFormat(DateTime::RFC3339, $phoneUnBilledCall->callDate);

                    //если дата звонка ранее чем тот что положили в массив - пропускаем
                    if (isset($phoneBaseStations[$phoneNumber]))
                    {
                        $baseStationDate = DateTime::createFromFormat(
                            'Y-m-d H:i:s',
                            $phoneBaseStations[$phoneNumber]['callDate']
                        );

                        if ($baseStationDate >= $callDate) {
                            continue;
                        }
                    }

                    //если уже определили что номер на Кавказе но в этом же файле есть записи Вся Россия
                    // - то оставляем Кавказ
                    if (isset($phoneBaseStations[$phoneNumber])
                        && $phoneBaseStations[$phoneNumber]['regionId'] !== self::BASE_STATIONS_REGION_RUSSIA
                        && $regionId === self::BASE_STATIONS_REGION_RUSSIA
                    )
                    {
                        continue;
                    }

                    $phoneBaseStations[$phoneNumber] = [
                        'regionId' => $regionId,
                        'callDate' => $callDate->format('Y-m-d H:i:s'),
                    ];
                }
            }
        }

        $rows = $this->getRegionArrays($phoneBaseStations);
        $saleRows = $rows['sale'];
        $locationRows = $rows['location'];

        $this->batchInsertToDb($saleRows, 'region_sale_id', 'region_sale_id_date');
        $this->batchInsertToDb($locationRows, 'region_location_id', 'region_location_id_date');
    }

    /**
     * Пакетная запись в OperatorParamsEntity::tableName()
     * @return bool
     * @throws \yii\db\Exception
     */
    private function batchInsertToDb(
        array $rows,
        string $updateAttributeId = 'region_sale_id',
        string $updateAttributeDate = 'region_sale_id_date'
    ): bool {
        if (count($rows) === 0) {
            return false;
        }

        $command = OperatorParamsEntity::getDb()
            ->createCommand()
            ->batchInsert(
                OperatorParamsEntity::tableName(),
                (new OperatorParamsEntity())->attributes(),
                $rows
            );

        $command->sql .= ' ON DUPLICATE KEY UPDATE 
                        ' . $updateAttributeId . ' = VALUES(' . $updateAttributeId . '), 
                        ' . $updateAttributeDate . ' = VALUES(' . $updateAttributeDate . ')';

        if ($command->execute()) {
            return true;
        }
        return false;
    }

    /**
     * @param array $phoneBaseStations массив в формате
     * [
     *     '9629993334' => [
     *         'regionId' => 1000,
     *         'callDate' => '2021-09-29 13:45:45',
     *     ],
     * .....
     * ]
     *
     * @return array[]
     * @throws Exception
     */
    private function getRegionArrays(array $phoneBaseStations): array
    {
        $saleRows = [];
        $locationRows = [];

        $operatorRegions = OperatorParamsEntity::find()
            ->where(['phone_number' => array_keys($phoneBaseStations)])
            ->indexBy('phone_number')
            ->asArray()
            ->all();

        foreach ($phoneBaseStations as $phoneNumber => $regionData) {
            $phoneBaseStationRegionId = $regionData['regionId'];
            $phoneBaseStationCallDate = $regionData['callDate'];

            $operatorRegion = new OperatorParamsEntity();
            $operatorRegion->setAttributes($operatorRegions[$phoneNumber] ?? []);

            $row = [
                $phoneNumber,
                $phoneBaseStationRegionId,
                $phoneBaseStationCallDate,
                $phoneBaseStationRegionId,
                $phoneBaseStationCallDate
            ];

            //если на номере еще не установлен регион продажи
            //либо регион продажи = Вся Россия и с момента активации не более двух дней
            //то обновляем регион продажи
            if (empty($operatorRegion->region_sale_id)
                || (
                (int)$operatorRegion->region_sale_id === self::BASE_STATIONS_REGION_RUSSIA
                    && $phoneBaseStationRegionId !== self::BASE_STATIONS_REGION_RUSSIA
                    && (new DateTime($operatorRegion->region_sale_id_date)) >= ((new DateTime())->sub(new DateInterval('P2D')))
                )
            ) {
                $saleRows[] = $row;
            }

            //регион местонахождения
            if (empty($operatorRegion->region_location_id)
                || (new DateTime($phoneBaseStationCallDate)) > (new DateTime($operatorRegion->region_location_id_date))
            ) {
                $locationRows[] = $row;
            }
        }

        return [
            'sale' => $saleRows,
            'location' => $locationRows,
        ];
    }
}
